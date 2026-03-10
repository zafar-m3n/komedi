const swaggerJsdoc = require("swagger-jsdoc");

const options = {
  definition: {
    openapi: "3.0.0",
    info: {
      title: "Submit Joke API",
      version: "1.0.0",
      description: "API for submitting jokes",
    },
    servers: [
      {
        url: "http://localhost:4200",
      },
    ],
  },
  apis: ["./server.js"],
};

const swaggerSpec = swaggerJsdoc(options);

module.exports = swaggerSpec;
