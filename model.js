"use strict";

const mongoose = require("mongoose");

const StockEtablissementModelSchema = new mongoose.Schema({
    siren: { type: String, required: false, trim: true },
    nic: { type: String, required: false, trim: true },
    siret: { type: String, required: false, trim: true },
    statutDiffusionEtablissement: { type: String, required: false, trim: true },
    dateDernierTraitementEtablissement: { type: String, required: false, trim: true },
    typeVoieEtablissement: { type: String, required: false, trim: true },
    libelleVoieEtablissement: { type: String, required: false, trim: true },
    codePostalEtablissement: { type: String, required: false, trim: true },
    libelleCommuneEtablissement: { type: String, required: false, trim: true },
    codeCommuneEtablissement: { type: String, required: false, trim: true },
    dateDebut: { type: String, required: false, trim: true },
    etatAdministratifEtablissement: { type: String, required: false, trim: true }    
});

exports.StockEtablissementModel = mongoose.model("StockEtab", StockEtablissementModelSchema);

