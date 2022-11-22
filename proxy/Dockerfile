FROM debian:bullseye

EXPOSE 80

RUN apt-get update \
    && apt-get purge nginx \
    && apt-get -y install --no-install-recommends wget gnupg ca-certificates \
    && wget -O - https://openresty.org/package/pubkey.gpg | apt-key add - \
    && codename=`grep -Po 'VERSION="[0-9]+ \(\K[^)]+' /etc/os-release` \
    && echo "deb http://openresty.org/package/debian $codename openresty" > /etc/apt/sources.list.d/openresty.list \
    && apt-get update \
    && apt-get -y install --no-install-recommends openresty

COPY nginx.conf /etc/openresty/
COPY default.conf /etc/openresty/conf.d/

CMD ["openresty", "-g", "daemon off;"]
