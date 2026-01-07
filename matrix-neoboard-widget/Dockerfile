FROM node:24-alpine AS build
WORKDIR /repo

COPY . .

RUN yarn install --frozen-lockfile
RUN yarn workspace @nordeck/matrix-neoboard-widget build

FROM nginx:1.27-alpine AS runtime

ARG REACT_APP_VERSION
ARG REACT_APP_REVISION
ENV REACT_APP_VERSION=${REACT_APP_VERSION}
ENV REACT_APP_REVISION=${REACT_APP_REVISION}

RUN rm -rf /docker-entrypoint.d/*

COPY ./matrix-neoboard-widget/nginx.conf /etc/nginx/conf.d/default.conf

RUN sed -i '1i pid /tmp/nginx.pid;' /etc/nginx/nginx.conf

COPY --from=build /repo/matrix-neoboard-widget/build/ /usr/share/nginx/html/

EXPOSE 8080
