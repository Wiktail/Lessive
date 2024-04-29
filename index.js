const fs = require('fs')
const pm2 = require('pm2')
const mongoose = require('mongoose')
const {split, countFilesInDirectory} = require('./csvHelper');


split("StockEtablissement_utf8.csv").then(() =>{
    countFilesInDirectory("./csv").then(count => {
        mongoose.connect("mongodb://127.0.0.1:27017/sirenes").then(r => {
            let i = 0;
            let indexMax = count;
            let totalTime = 0;
            pm2.connect((err) => {
                if (err) {
                    console.error(err)
                    process.exit(2)
                }
        
                pm2.launchBus((workerError, bus) => {
                    bus.on('process:msg', (packet) => {
                        let workerId = packet.process.pm_id
                        console.log("message received from worker n°", workerId)
                        let data = packet.data
                        if (!!data.time) {
                            console.log("time to insert file:", data.time, "s")
                            totalTime += data.time
                        }
                        console.log("current index =", i)
                        if (i < indexMax) {
                            console.log(indexMax);
                            pm2.sendDataToProcessId(workerId, {
                                id: workerId,
                                type: 'process:msg',
                                data: {
                                    file: `csv/${i++}.csv`
                                },
                                topic: true
        
                            }, (messageError) => {
                                if (messageError) {
                                    console.error(`error when messaging ${workerId}`, messageError);
                                }
                            })
                        } else {
                            console.log("stop worker n°", workerId)
                            pm2.delete(workerId, (stopError) => {
                                if (stopError) {
                                    console.error('error when stopping', stopError)
                                }
                            })
        
                            pm2.list((err, processDescriptionArray) => {
        
                                let workerExists = processDescriptionArray.some(proc => proc.name === "worker");
        
                                if (!workerExists) {
                                    console.log("TotalTime", totalTime);
                                }
                            })
                        }
                    })
                })
        
                pm2.start({
                    script: 'worker.js',
                    name: `worker`,
                    exec_mode: 'cluster',
                    instances: 'max'
                }, (error, proc) => {
                    if (error) {
                        console.error(error)
                        pm2.disconnect()
                    }
                })
            })
        })
    })

})


