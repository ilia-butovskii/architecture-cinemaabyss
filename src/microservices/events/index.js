const express = require("express");
const { Kafka } = require("kafkajs");

init();

async function init() {
    const app = express();

    const kafka = new Kafka({
        clientId: "events-service",
        brokers: process.env.KAFKA_BROKERS.split(","),
    });

    console.log(process.env.KAFKA_BROKERS.split(","));

    const producer = kafka.producer();
    await producer.connect();

    initRouters(app, producer);

    await initConsumers(kafka);

    app.listen(process.env.PORT, () => {
        console.log(`Server is running on port ${process.env.PORT}`);
    });
}

function initRouters(app, producer) {
    app.get("/api/events/health", (req, res) => {
        res.status(200).json({ status: true });
    });

    app.post("/api/events/movie", async (req, res) => {
        await producer.send({
            topic: "movie-events",
            messages: [{value: 'Movie'}],
        });

        res.status(201).json({ status: "success" });
    });

    app.post("/api/events/user", async (req, res) => {
        await producer.send({
            topic: "user-events",
            messages: [{value: 'User'}],
        });
        res.status(201).json({ status: "success" });
    });

    app.post("/api/events/payment", async (req, res) => {
        await producer.send({
            topic: "payment-events",
            messages: [{value: 'Payment'}],
        });
        res.status(201).json({ status: "success" });
    });
}

async function initConsumers(kafka) {
    const movieConsumer = kafka.consumer({ groupId: "movie-events-consumer" });
    const userConsumer = kafka.consumer({ groupId: "user-events-consumer" });
    const paymentConsumer = kafka.consumer({ groupId: "payment-events-consumer" });

    await movieConsumer.subscribe({ topic: "movie-events", fromBeginning: true });
    await userConsumer.subscribe({ topic: "user-events", fromBeginning: true });
    await paymentConsumer.subscribe({ topic: "payment-events", fromBeginning: true });

    [movieConsumer, userConsumer, paymentConsumer].forEach(async (consumer) => {
        await consumer.run({
            eachMessage: async ({ topic, partition, message }) => {
                console.log({ topic, partition, message: message.value.toString() });
            },
        });
    });
}