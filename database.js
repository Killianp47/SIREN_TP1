"use strict";

const mongoose = require("mongoose");
const stock = require("./model");

let db;

const createConnection = async (truncate = false) => {
    
    var mongoDB = "mongodb://localhost:27017/sirene";
    db = await mongoose.connect(mongoDB, { useNewUrlParser: true, useUnifiedTopology: true, minPoolSize: 5, maxPoolSize: 100 });
    console.log("Connexion réussi");

    if (truncate) {
        console.log("Truncating DB");
        await stock.StockEtablissementModel.deleteMany({});
        console.log("DB Truncate réussi");
    }

};

const closeConnection = async () => {
    console.log("Fermeture de la connexion");
    await db.disconnect();
};

module.exports = {
    createConnection,
    closeConnection
};