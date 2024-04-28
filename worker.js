const fs = require('fs');
const csv_to_table = require('./splitcsv.js');
const stock = require("./model");
const mongoose = require("mongoose");
const db = require("./database");

var WORKING_FILE = "";

function MarkAsCompleted() {
    process.send({
        type: 'process:msg',
        data: {
            FILE: WORKING_FILE,
            PID: process.env.pm_id
        }
    });
}

function MarkAsReady() {
    process.send({
        type: 'process:msg',
        data: {
            READY: true,
            PID: process.env.pm_id
        }
    });
}

MarkAsReady();

process.on('message', async function (packet) {
    try {
        await db.createConnection();
        WORKING_FILE = packet.data.FILE;
        const csvStr = fs.readFileSync(WORKING_FILE);
        const models = csvStr.toString().split("\n").map((line) => {
            const data = line.split(",");
            const stock = {
                _id: new mongoose.Types.ObjectId().toString(),
                siren: data[0].replace(/['"]+/g, '') || undefined,
                nic: data[1].replace(/['"]+/g, '') || undefined, 
                siret: data[2].replace(/['"]+/g, '') || undefined, 
                dateCreationEtablissement: data[4].replace(/['"]+/g, '') || undefined, 
                dateDernierTraitementEtablissement: data[8].replace(/['"]+/g, '') || undefined,
                typeVoieEtablissement: data[14].replace(/['"]+/g, '') || undefined,
                libelleVoieEtablissement: data[15].replace(/['"]+/g, '') || undefined,
                codePostalEtablissement: data[16].replace(/['"]+/g, '') || undefined,
                libelleCommuneEtablissement: data[17].replace(/['"]+/g, '') || undefined,
                codeCommuneEtablissement: data[20].replace(/['"]+/g, '') || undefined,
                dateDebut: data[39].replace(/['"]+/g, '') || undefined,
                etatAdministratifEtablissement: data[40].replace(/['"]+/g, '') || undefined
            };
            return Object.fromEntries(Object.entries(stock).filter(([, value]) => value !== undefined));
        });
        if (models) {
            console.log(`Inserting from ${WORKING_FILE} : ${models.length}`);
            const data = await stock.StockEtablissementModel.collection.insertMany(models);
            console.log(`Inserted ${data.insertedCount} documents in MongoDB`);
            await db.closeConnection();
        }
    } catch (err) {
        console.error(err)
    }
    MarkAsCompleted();
});