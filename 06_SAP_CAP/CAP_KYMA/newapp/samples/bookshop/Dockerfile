FROM node:14.18.1

WORKDIR /usr/src/app

COPY /package*.json ./

RUN npm install

COPY /. .

EXPOSE 4004

CMD ["npm", "start"]