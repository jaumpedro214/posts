FROM node:20-alpine

WORKDIR /app
RUN npm install -g live-server

COPY . /app

CMD ["live-server", "--port=8080", "--host=0.0.0.0", "--no-browser", "--entry-file=main.html"]