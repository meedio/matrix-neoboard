/*
 * Copyright 2025 Nordeck IT + Consulting GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import { StateEvent, WidgetApi } from '@matrix-widget-toolkit/api';
import { clone, isEqual, isError } from 'lodash';
import { getLogger } from 'loglevel';
import { UpdateDelayedEventAction } from 'matrix-widget-api';
import {
  Observable,
  Subject,
  filter,
  from,
  interval,
  switchMap,
  takeUntil,
} from 'rxjs';
import {
  RTCSessionEventContent,
  STATE_EVENT_RTC_MEMBER,
  isRTCSessionNotExpired,
  newRTCSession,
} from '../../../model';
import { DEFAULT_RTC_EXPIRE_DURATION } from '../../../model/matrixRtcSessions';
import {
  RTCFocus,
  getWellKnownFoci,
  makeFociPreferred,
} from './matrixRtcFocus';
import { SessionState } from './sessionManagerImpl';
import { MatrixRtcSessionManager, Session } from './types';

type RTCSessionRoomEventContent = RTCSessionEventContent & {
  session_id: string;
  ended?: boolean;
};

// IMPORTANT: use toolkit StateEvent so it matches isRTCSessionNotExpired(...)
type RTCSessionRoomEvent = StateEvent<RTCSessionRoomEventContent>;

function isRTCSessionRoomEvent(ev: unknown): ev is RTCSessionRoomEvent {
  if (!ev || typeof ev !== 'object') return false;
  const obj = ev as Record<string, unknown>;
  const content = obj.content as Record<string, unknown> | undefined;

  return (
    typeof obj.type === 'string' &&
    typeof obj.sender === 'string' &&
    typeof obj.room_id === 'string' &&
    !!content &&
    typeof content === 'object' &&
    typeof content.session_id === 'string' &&
    typeof content.call_id === 'string'
  );
}

/**
 * Minimal “extra methods” surface used by this manager.
 * These methods may or may not exist depending on widget API implementation / capabilities.
 */
type WidgetApiRtcExtensions = {
  observeRoomEvents: (eventType: string) => Observable<unknown>;
  receiveRoomEvents: (
    eventType: string,
  ) => Promise<unknown[] | null | undefined>;
  sendEvent: (eventType: string, content: unknown) => Promise<void>;

  // Delayed events variants
  sendDelayedEvent: (
    eventType: string,
    content: unknown,
    delayMs: number,
  ) => Promise<{ delay_id: string }>;
  sendDelayedStateEvent: (
    eventType: string,
    content: unknown,
    delayMs: number,
    opts: { stateKey: string },
  ) => Promise<{ delay_id: string }>;
  updateDelayedEvent: (
    delayId: string,
    action: UpdateDelayedEventAction,
  ) => Promise<void>;
};

function hasFn<K extends keyof WidgetApiRtcExtensions>(
  api: unknown,
  key: K,
): api is Pick<WidgetApiRtcExtensions, K> {
  return !!api && typeof (api as Record<string, unknown>)[key] === 'function';
}

export class MatrixRtcSessionManagerImpl implements MatrixRtcSessionManager {
  private readonly logger = getLogger('RTCSessionManager');
  private readonly destroySubject = new Subject<void>();
  private readonly leaveSubject = new Subject<void>();
  private readonly sessionJoinedSubject = new Subject<Session>();
  private readonly sessionLeftSubject = new Subject<Session>();
  private readonly activeFocusSubject = new Subject<RTCFocus>();
  private readonly sessionSubject = new Subject<SessionState>();

  private sessions: RTCSessionRoomEvent[] = [];
  private joinState: { whiteboardId: string; sessionId: string } | undefined;
  private fociPreferred: RTCFocus[] = [];
  private wellKnownFoci: RTCFocus[] = [];
  private activeFocus: RTCFocus | undefined;

  /**
   * Holds remove membership event delay id.
   * Is undefined if homeserver doesn't support delayed events (or we can't use it here).
   * Is assigned undefined if cannot refresh a delayed event with this id.
   */
  private removeSessionDelayId?: string;

  constructor(
    private readonly widgetApiPromise: Promise<WidgetApi> | WidgetApi,
    private readonly sessionTimeout = DEFAULT_RTC_EXPIRE_DURATION,
    private readonly wellKnownPollingInterval = 60 * 1000,
    private readonly removeSessionDelay: number = 8000,
  ) {}

  initFociDiscovery(): void {
    this.checkForWellKnownFoci().catch((error) => {
      this.logger.error('Failed to check for well-known foci:', error);
    });

    interval(this.wellKnownPollingInterval)
      .pipe(takeUntil(this.destroySubject))
      .subscribe(async () => {
        await this.checkForWellKnownFoci();
      });
  }

  getSessionId(): string | undefined {
    return this.joinState?.sessionId;
  }

  getSessions(): Session[] {
    return this.sessions.map((ev) => ({
      sessionId: ev.content.session_id,
      userId: ev.sender,
    }));
  }

  getActiveFocus(): RTCFocus | undefined {
    return this.activeFocus;
  }

  getRemoveSessionDelayId(): string | undefined {
    return this.removeSessionDelayId;
  }

  observeSessionJoined(): Observable<Session> {
    return this.sessionJoinedSubject;
  }

  observeSessionLeft(): Observable<Session> {
    return this.sessionLeftSubject;
  }

  observeActiveFocus(): Observable<RTCFocus> {
    return this.activeFocusSubject;
  }

  observeSession(): Observable<SessionState> {
    return this.sessionSubject;
  }

  async join(whiteboardId: string): Promise<{ sessionId: string }> {
    if (this.joinState) {
      this.logger.debug('Already joined a whiteboard, must leave first.');
      await this.leave();
    }

    const widgetApi = await this.widgetApiPromise;
    const { userId, deviceId } = widgetApi.widgetParameters;
    const sessionId = `_${userId}_${deviceId}`;

    this.logger.debug(
      `Joining whiteboard ${whiteboardId} as session ${sessionId}`,
    );

    interval(this.sessionTimeout * 0.75)
      .pipe(
        takeUntil(this.destroySubject),
        takeUntil(this.leaveSubject),
        switchMap(() => this.refreshOwnSession(sessionId, whiteboardId)),
      )
      .subscribe();

    await this.refreshOwnSession(sessionId, whiteboardId);
    await this.scheduleRemoveMembershipDelayedEvent(widgetApi, sessionId);

    from(Promise.resolve(this.widgetApiPromise))
      .pipe(
        switchMap((api) => {
          if (!hasFn(api, 'observeRoomEvents')) {
            return new Observable<unknown>((subscriber) =>
              subscriber.complete(),
            );
          }
          return api.observeRoomEvents(STATE_EVENT_RTC_MEMBER);
        }),
        filter(isRTCSessionRoomEvent),
        takeUntil(this.destroySubject),
        takeUntil(this.leaveSubject),
      )
      .subscribe(async (rtcSession: RTCSessionRoomEvent) => {
        if (rtcSession.content.call_id !== whiteboardId) return;

        if (
          rtcSession.content.ended === true ||
          Object.keys(rtcSession.content).length === 0
        ) {
          this.removeSession(rtcSession.content.session_id, rtcSession.sender);
          if (rtcSession.content.session_id === sessionId) {
            await this.refreshOwnSession(sessionId, whiteboardId);

            if (!this.removeSessionDelayId || rtcSession.sender !== userId) {
              await this.scheduleRemoveMembershipDelayedEvent(
                widgetApi,
                sessionId,
              );
            }
          }
          return;
        }

        this.handleRTCSessionEvent(rtcSession);
      });

    this.observeSessionLeft()
      .pipe(takeUntil(this.destroySubject), takeUntil(this.leaveSubject))
      .subscribe(async () => {
        await this.computeActiveFocus();
      });

    this.joinState = { sessionId, whiteboardId };

    return { sessionId };
  }

  /**
   * Sends a remove membership delayed event, updates delay id.
   * Refreshes a delayed event periodically.
   * Invalidates a delay id if failed to refresh.
   *
   * NOTE: this is best-effort. If the widget API / HS does not support delayed
   * room events, this becomes a no-op and we rely on expires.
   */
  private async scheduleRemoveMembershipDelayedEvent(
    widgetApi: WidgetApi,
    sessionId: string,
  ): Promise<void> {
    let removeSessionDelayId: string | undefined;

    try {
      const apiUnknown: unknown = widgetApi;

      if (hasFn(apiUnknown, 'sendDelayedEvent')) {
        ({ delay_id: removeSessionDelayId } = await apiUnknown.sendDelayedEvent(
          STATE_EVENT_RTC_MEMBER,
          { session_id: sessionId, ended: true },
          this.removeSessionDelay,
        ));
      } else if (hasFn(apiUnknown, 'sendDelayedStateEvent')) {
        ({ delay_id: removeSessionDelayId } =
          await apiUnknown.sendDelayedStateEvent(
            STATE_EVENT_RTC_MEMBER,
            {},
            this.removeSessionDelay,
            { stateKey: sessionId },
          ));
      }
    } catch (ex) {
      this.logger.error(
        'Could not send remove membership delayed event:',
        isError(ex) ? ex.message : ex,
      );
    }

    if (removeSessionDelayId) {
      interval(this.removeSessionDelay * 0.75)
        .pipe(
          takeUntil(this.destroySubject),
          takeUntil(this.leaveSubject),
          switchMap(() => {
            const apiUnknown: unknown = widgetApi;
            if (!hasFn(apiUnknown, 'updateDelayedEvent')) {
              return new Observable<void>((subscriber) =>
                subscriber.complete(),
              );
            }
            return from(
              apiUnknown.updateDelayedEvent(
                removeSessionDelayId,
                UpdateDelayedEventAction.Restart,
              ),
            );
          }),
        )
        .subscribe({
          error: (err) => {
            if (
              this.removeSessionDelayId &&
              this.removeSessionDelayId === removeSessionDelayId
            ) {
              this.removeSessionDelayId = undefined;
            }
            this.logger.error(
              'Could not refresh delayed event:',
              isError(err) ? err.message : err,
            );
          },
        });

      this.removeSessionDelayId = removeSessionDelayId;
    }
  }

  async leave(): Promise<void> {
    if (!this.joinState) {
      return;
    }
    const { sessionId, whiteboardId } = this.joinState;

    this.joinState = undefined;
    this.leaveSubject.next();

    this.logger.log(
      `Leaving whiteboard ${whiteboardId} as session ${sessionId}`,
    );

    const widgetApi = await this.widgetApiPromise;
    const { userId } = widgetApi.widgetParameters;

    if (userId) {
      this.removeSession(sessionId, userId);
    }

    await this.endRtcSession(sessionId);

    if (this.removeSessionDelayId) {
      const apiUnknown: unknown = widgetApi;
      if (hasFn(apiUnknown, 'updateDelayedEvent')) {
        apiUnknown.updateDelayedEvent(
          this.removeSessionDelayId,
          UpdateDelayedEventAction.Cancel,
        );
      }
      this.removeSessionDelayId = undefined;
    }
  }

  destroy(): void {
    this.destroySubject.next();
    this.sessionJoinedSubject.complete();
    this.sessionSubject.complete();
    this.sessionLeftSubject.complete();
    this.activeFocusSubject.complete();
  }

  private async checkForWellKnownFoci(): Promise<void> {
    this.logger.debug('Looking up the homeserver RTC foci');

    const widgetApi = await this.widgetApiPromise;
    const domain = widgetApi.widgetParameters.userId?.replace(/^.*?:/, '');

    const foci = await getWellKnownFoci(domain);
    this.logger.debug('Found homeserver foci', JSON.stringify(foci));

    if (!isEqual(foci, this.wellKnownFoci)) {
      this.logger.debug('Homeserver foci changed');
      this.wellKnownFoci = foci;
      await this.computeActiveFocus();
    } else {
      this.logger.debug('No new homeserver foci found');
    }
  }

  private async computeActiveFocus() {
    this.logger.debug('Checking if a new active focus is required');

    const memberFocus = await this.selectMemberFocus();

    this.fociPreferred = makeFociPreferred(memberFocus, this.wellKnownFoci);

    const newActiveFocus = this.fociPreferred[0];
    if (!isEqual(this.activeFocus, newActiveFocus)) {
      this.logger.debug('New active focus:', newActiveFocus);
      this.activeFocus = newActiveFocus;
      this.activeFocusSubject.next(newActiveFocus);
    }
  }

  private async selectMemberFocus(): Promise<RTCFocus | undefined> {
    let sessions: RTCSessionRoomEvent[] = [];

    if (!this.joinState) {
      this.logger.debug(
        'Not joined yet, need to retrieve session member room events',
      );
      try {
        const widgetApi = await this.widgetApiPromise;
        const apiUnknown: unknown = widgetApi;

        // best-effort: if receiveRoomEvents exists use it; else fall back to cache (empty).
        if (hasFn(apiUnknown, 'receiveRoomEvents')) {
          const evs = await apiUnknown.receiveRoomEvents(
            STATE_EVENT_RTC_MEMBER,
          );
          sessions = (evs ?? []).filter(isRTCSessionRoomEvent);
        } else {
          sessions = [];
        }
      } catch (error) {
        this.logger.error(
          'Failed to receive session member room events',
          error,
        );
        return;
      }
    } else {
      this.logger.debug(
        'Already joined, using cached session member room events',
      );
      sessions = this.sessions;
    }

    sessions = sessions.filter(isRTCSessionNotExpired);

    if (sessions.length < 1) {
      this.logger.debug('No member focus to check, skipping');
      return;
    }

    const sortedSessions = sessions.sort((a, b) => {
      const aExpire = a.content.expires || Infinity;
      const bExpire = b.content.expires || Infinity;
      return aExpire - bExpire;
    });

    const oldestSession = sortedSessions[0];
    this.logger.debug(
      'Found oldest session:',
      oldestSession.content.session_id,
    );

    if (oldestSession?.content?.session_id) {
      if (
        oldestSession.content.focus_active.type === 'livekit' &&
        oldestSession.content.focus_active.focus_selection ===
          'oldest_membership'
      ) {
        if (oldestSession.content.session_id === this.getSessionId()) {
          return undefined;
        } else {
          const newMemberFocus = oldestSession.content.foci_preferred[0];
          this.logger.debug('New member focus:', newMemberFocus);
          return newMemberFocus;
        }
      } else {
        this.logger.error(
          'Unsupported focus selection type on oldest session member',
        );
      }
    }
  }

  private handleRTCSessionEvent(event: RTCSessionRoomEvent): void {
    const sessionId = event.content.session_id;
    const whiteboardId = event.content.call_id;

    this.logger.debug('Handling RTC event', JSON.stringify(event));

    this.sessionSubject.next({
      sessionId,
      userId: event.sender,
      expiresTs: event.content.expires,
      whiteboardId,
    });

    if (
      event.content.ended === true ||
      Object.keys(event.content).length === 0
    ) {
      this.removeSession(sessionId, event.sender);
      return;
    }

    this.sessions = this.sessions.filter(isRTCSessionNotExpired);

    const existingSessionIndex = this.sessions.findIndex(
      (s) =>
        s.content.session_id === sessionId &&
        s.content.call_id === whiteboardId,
    );

    if (existingSessionIndex >= 0) {
      this.sessions[existingSessionIndex] = event;
    } else {
      if (sessionId !== this.getSessionId()) {
        this.addSession(event);
      } else {
        this.sessions.push(event);
      }
    }

    this.logger.debug('Sessions updated', JSON.stringify(this.sessions));
  }

  private addSession(session: RTCSessionRoomEvent): void {
    const { sender } = session;

    this.logger.debug(
      `Session ${session.content.session_id} by ${sender} joined whiteboard ${session.content.call_id}`,
    );

    this.sessions = [...this.sessions, session];
    this.sessionJoinedSubject.next({
      sessionId: session.content.session_id,
      userId: sender,
    });
  }

  private removeSession(sessionId: string, userId: string): void {
    this.logger.debug(`Session ${sessionId} left whiteboard`);

    this.sessions = this.sessions.filter(
      (s) => s.content.session_id !== sessionId,
    );
    this.sessionLeftSubject.next({
      sessionId,
      userId,
    });
  }

  private async refreshOwnSession(
    sessionId: string | undefined,
    whiteboardId: string,
  ): Promise<void> {
    const expires = Date.now() + this.sessionTimeout;
    const widgetApi = await this.widgetApiPromise;
    const { userId, deviceId } = widgetApi.widgetParameters;

    this.logger.debug(`Refreshing session ${sessionId}`);

    if (!userId || !deviceId || !whiteboardId || !sessionId) {
      this.logger.error(
        'Unknown user id or device id or whiteboard id when patching RTC session',
      );
      throw new Error('Unknown user id or device id or whiteboard id');
    }

    try {
      const cached = this.sessions.find(
        (s) =>
          s.content.session_id === sessionId &&
          s.content.call_id === whiteboardId,
      );

      let baseSession: RTCSessionRoomEventContent;

      if (
        cached &&
        cached.content &&
        Object.keys(cached.content).length !== 0
      ) {
        baseSession = clone(cached.content);
      } else {
        baseSession = {
          ...(newRTCSession(deviceId, whiteboardId) as RTCSessionEventContent),
          session_id: sessionId,
        };
      }

      const foci_preferred = this.fociPreferred.map((focus) => {
        if (focus.type === 'livekit') {
          return {
            ...focus,
            livekit_alias: widgetApi.widgetParameters.roomId,
          };
        }
        return focus;
      });

      const updatedSession: RTCSessionRoomEventContent = {
        ...baseSession,
        session_id: sessionId,
        expires,
        foci_preferred,
        ended: false,
      };

      const prev = cached?.content;

      if (!isEqual(updatedSession, prev)) {
        const apiUnknown: unknown = widgetApi;
        if (!hasFn(apiUnknown, 'sendEvent')) {
          throw new Error('WidgetApi missing sendEvent');
        }
        await apiUnknown.sendEvent(STATE_EVENT_RTC_MEMBER, updatedSession);
        this.logger.debug(
          'RTC session room-event sent',
          JSON.stringify(updatedSession),
        );
      }
    } catch (ex) {
      this.logger.error('Error while sending RTC session', ex);
    }
  }

  private async endRtcSession(sessionId: string): Promise<void> {
    const widgetApi = await this.widgetApiPromise;
    const { userId, deviceId } = widgetApi.widgetParameters;

    this.logger.debug(
      'Ending RTC session with ended marker (room event)',
      userId,
      deviceId,
      sessionId,
    );

    try {
      const apiUnknown: unknown = widgetApi;
      if (!hasFn(apiUnknown, 'sendEvent')) {
        throw new Error('WidgetApi missing sendEvent');
      }
      await apiUnknown.sendEvent(STATE_EVENT_RTC_MEMBER, {
        session_id: sessionId,
        ended: true,
      } as RTCSessionRoomEventContent);
    } catch (ex) {
      this.logger.error('Error while ending RTC session', ex);
    }
  }
}
