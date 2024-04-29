const {SireneModel, parseCSV} = require('./mongooseHelper')
const mongoose = require("mongoose");


console.log("worker started")

mongoose.connect("mongodb://127.0.0.1:27017/sirenes").then(() => {
    process.on("message", (packet) => {
        if (packet.type === "play") {
            process.send({
                type: 'process:msg',
                data: {}
            })
            return;
        }

        let start = process.hrtime()

        let data = packet.data
        console.log("file to read:", data.file)

        parseCSV(data.file).then((documents) => {

            SireneModel.bulkWrite(documents.map((document) => ({
                insertOne: {
                    document: document,
                },
            })))
                .then(() => {
                    const end = process.hrtime(start); // Enregistrer le temps de fin du traitement
                    const durationMs = (end[0] * 1e9 + end[1]) / 1e6; // Convertir la durée en millisecondes
                    const durationSec = Math.floor(durationMs / 1000); // Convertir la durée en secondes et arrondir vers le bas

                    process.send({
                        type: 'process:msg',
                        data: {
                            time: durationSec
                        }
                    })
                })
                .catch((error) => {
                    console.error(error.message)
                })
        })
    })

    process.send({
        type: 'process:msg',
        data: {
            state: 'idle'
        }
    })

})

