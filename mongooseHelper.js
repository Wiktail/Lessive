const mongoose = require("mongoose");
const fs = require("fs");
const readline = require("readline");

const sireneSchema = new mongoose.Schema({
    siren: String,
    nic: String,
    siret: String,
    dateCreationEtablissement: Date,
    dateDernierTraitementEtablissement: Date,
    typeVoieEtablissement: String,
    libelleVoieEtablissement: String,
    codePostalEtablissement: String,
    libelleCommuneEtablissement: String,
    codeCommuneEtablissement: String,
    dateDebut: Date,
    etatAdministratifEtablissement: String,
});

const SireneModel = mongoose.model("SireneModel", sireneSchema);

function cleanData(value, type = "string") {
    if (value === "" || value === undefined || value === null) {
        return null;
    }
    try {
        if (type === "date") {
            const date = new Date(value);
            return isNaN(date.getTime()) ? null : date;
        }
    } catch (error) {
        return null;
    }
    return value;
}


function parseCSV(filePath) {
    return new Promise((resolve, reject) => {
        const documents = [];
        const fileStream = fs.createReadStream(filePath);

        const rl = readline.createInterface({
            input: fileStream,
            crlfDelay: Infinity,
        });

        let isFirstLine = true;

        rl.on('line', (line) => {
//            console.log('line', line);

            if (isFirstLine) {
                isFirstLine = false;
                return; // Ignore la première ligne
            }

            const columns = line.split(',');

            console.log(columns[43])

            documents.push({
                siren: cleanData(columns[0]),
                nic: cleanData(columns[1]),
                siret: cleanData(columns[2]),
                dateCreationEtablissement: cleanData(columns[3], "date"),
                dateDernierTraitementEtablissement: cleanData(columns[4], "date"),
                typeVoieEtablissement: cleanData(columns[5]),
                libelleVoieEtablissement: cleanData(columns[6]),
                codePostalEtablissement: cleanData(columns[7]),
                libelleCommuneEtablissement: cleanData(columns[8]),
                codeCommuneEtablissement: cleanData(columns[9]),
                dateDebut: cleanData(columns[10], "date"),
                etatAdministratifEtablissement: cleanData(columns[11]),
            });
        });

        rl.on('close', () => {
            resolve(documents);
        });

        rl.on('error', (err) => {
            reject(err);
        });
    });
}


module.exports = {SireneModel, parseCSV};
