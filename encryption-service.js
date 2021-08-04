'use strict';

const write = require('fs-writefile-promise');
const fs = require('fs');
const csv = require('csv-parser');
const {sendKey, getKey} = require("./service");
const util = require("./util");
const es = require('event-stream');
const utf8 = require('utf8');
const crypto = require("crypto-js");
const stringify = require('csv-stringify');
let LOGGER = require('simple-node-logger').createSimpleLogger({
    logFilePath: `${__dirname}/logs.log`,
    timestampFormat: 'YYYY-MM-DD HH:mm:ss.SSS'
});

let rawData = fs.readFileSync(process.argv[2]);
let data = JSON.parse(rawData);
let input_file = data.input_file;
let configuration_fields = data.configuration_fields;
let job = data.job;
let token = data.token;
let userToken = data.userToken;
let api = "https://platform.icarus2020.aero/api/v1";

getMinMaxDistinctValuesForEncrypted(input_file, configuration_fields)
    .then((results) => write(`${input_file.substring(0, input_file.indexOf('.csv'))}_encrypted.json`, JSON.stringify(results)))
    .then(() => encryptData(configuration_fields, input_file, job))
    .catch(err => {
        console.log(err);
    });

function getMinMaxDistinctValuesForEncrypted(path, configuration_fields) {
    LOGGER.info("Get Min Max Distinct Values for encrypted");
    return new Promise((resolve, reject) => {
        LOGGER.info(`path ${path}`);
        let minMaxValues = {};
        let distinctValues = {};
        for (let field of configuration_fields) {
            if (field.encrypted && field.searchable) {
                if ((field.type === 'integer' || field.type === 'double') && !field.anonymization.transformation) {
                    if (field.mapped_field.indexOf('_') > 0) {
                        minMaxValues[`${field.icarus_field}Min_${field.order}`] = Number.MAX_VALUE;
                        minMaxValues[`${field.icarus_field}Max_${field.order}`] = Number.MIN_VALUE;
                    } else {
                        minMaxValues[`${field.mapped_field}Min`] = Number.MAX_VALUE;
                        minMaxValues[`${field.mapped_field}Max`] = Number.MIN_VALUE;
                    }

                } else if (field.type === 'date' || field.type === 'datetime') {
                    if (field.mapped_field.indexOf('_') > 0) {
                        minMaxValues[`${field.icarus_field}Min_${field.order}`] = moment(8640000000000000).format();
                        minMaxValues[`${field.icarus_field}Max_${field.order}`] = moment(-8640000000000000).format();
                    } else {
                        minMaxValues[`${field.mapped_field}Min`] = moment(8640000000000000).format();
                        minMaxValues[`${field.mapped_field}Max`] = moment(-8640000000000000).format();
                    }

                } else {
                    distinctValues[field.mapped_field] = [];
                }
            }
        }
        fs.createReadStream(path)
            .on("error", error => {
                reject({message: "getMinMaxDistictValuesForEncrypted", code: -1});
            })
            .pipe(csv({separator: ","}))
            .on('data', (row) => {
                for (let field of configuration_fields) {
                    if (row[field.mapped_field] !== undefined && field.encrypted && field.searchable) {
                        if (field.type === 'integer' || field.type === 'double') {
                            if (field.mapped_field.indexOf('_') > 0) {
                                minMaxValues[`${field.icarus_field}Max_${field.order}`] = util.getMaxNumber(row[field.mapped_field],
                                    minMaxValues[`${field.icarus_field}Max_${field.order}`]);
                                minMaxValues[`${field.icarus_field}Min_${field.order}`] = util.getMinNumber(row[field.mapped_field],
                                    minMaxValues[`${field.icarus_field}Min_${field.order}`]);
                            } else {
                                minMaxValues[`${field.mapped_field}Max`] = util.getMaxNumber(row[field.mapped_field],
                                    minMaxValues[`${field.mapped_field}Max`]);
                                minMaxValues[`${field.mapped_field}Min`] = util.getMinNumber(row[field.mapped_field],
                                    minMaxValues[`${field.mapped_field}Min`]);
                            }
                        } else if (field.type === 'date' || field.type === 'datetime') {
                            if (field.mapped_field.indexOf('_') > 0) {
                                minMaxValues[`${field.icarus_field}Max_${field.order}`] = util.getMaxDate(row[field.mapped_field],
                                    minMaxValues[`${field.icarus_field}Max_${field.order}`]);
                                minMaxValues[`${field.icarus_field}Min_${field.order}`] = util.getMinDate(row[field.mapped_field],
                                    minMaxValues[`${field.icarus_field}Min_${field.order}`]);
                            } else {
                                minMaxValues[`${field.mapped_field}Max`] = util.getMaxDate(row[field.mapped_field],
                                    minMaxValues[`${field.mapped_field}Max`]);
                                minMaxValues[`${field.mapped_field}Min`] = util.getMinDate(row[field.mapped_field],
                                    minMaxValues[`${field.mapped_field}Min`]);
                            }
                        } else {
                            if (!distinctValues[field.mapped_field].includes(row[field.mapped_field])) {
                                distinctValues[field.mapped_field].push(row[field.mapped_field]);
                            }
                        }
                    }
                }
            })
            .on("end", () => {
                let results = {
                    distinctValues: distinctValues,
                    minMaxValues: minMaxValues
                };
                resolve(results);
            });
    });
}

function encryptData(configuration_fields, input_file, job) {
    return new Promise((resolve, reject) => {
        LOGGER.info("Start encryption");
        LOGGER.info("Type: " + job.type);
        //generates the key
        let secret_key = null;
        if (job.type == "NEW") {
            secret_key = util.secretPassword();
            LOGGER.info(`Generates the key ${secret_key}`);
            LOGGER.info(`dataset id: ${job.id}`)
            sendKey(api + '/worker/', secret_key, job.id, token, userToken)
                .then(res => startEncryption(configuration_fields, job.id, input_file, secret_key))
                .then(res => resolve())
                .catch(err => {
                    LOGGER.error("Key error " + err);
                    reject({message: "Key error", code: -1});
                });

        } else {
            getKey(api + '/worker/update/' + job.id, token, userToken)
                .then(res => {
                    LOGGER.info("res.data")
                    LOGGER.info(res.data)
                    if (res.data.update !== "null") {
                        secret_key = res.data.update;
                        return startEncryption(configuration_fields, job.id, input_file, secret_key);
                    } else {
                        secret_key = util.secretPassword();
                        LOGGER.info(`Generates the key ${secret_key}`);
                        LOGGER.info(`dataset id: ${job.id}`)
                        sendKey(api + '/worker/', secret_key, job.id, token, userToken)
                            .then(res => startEncryption(configuration_fields, job.id, input_file, secret_key))
                            .then(res => resolve())
                            .catch(err => {
                                LOGGER.error("Key error " + err);
                                reject({message: "Key error", code: -1});
                            });
                    }
                })
                .then(res => resolve())
                .catch(err => {
                    LOGGER.error("Get key error " + err);
                    reject({message: "get key error", code: -1});
                })

        }
    });


}

function startEncryption(configuration_fields, id, path, secret_key) {
    return new Promise((resolve, reject) => {
        let encryption_path = path.substring(0, path.indexOf('.csv')) + "_encrypted.csv";
        LOGGER.info(`path ${path}`);
        let j = 0;
        let records = [];
        let r = 0;
        let first = true;
        fs.createReadStream(path)
            .on("error", error => {
                reject({message: "encryptData", code: -1});
            })
            .pipe(csv({separator: ","}))
            .pipe(
                es.mapSync(row => {
                    j = j + 1;
                    try {
                        row = encryptRow(row, configuration_fields, secret_key);
                    } catch (e) {
                        LOGGER.error("Encrypt row " + e);
                        reject({message: "Problem with encrypt row", code: -1});
                    }
                    r++;
                    records.push(row);
                    if (r === 1000) {
                        writeRows(records, encryption_path, first);
                        r = 0;
                        records = [];
                        first = false;
                    }
                })
            )
            .on('end', () => {
                if (r !== 0) {
                    writeRows(records, encryption_path, first);
                }
                LOGGER.info(`${j} encrypted rows`);
                resolve();
            });
    });

}

function writeRows(records, encryption_path, header) {
    stringify(records, {
        header: header
    }, function (err, output) {
        fs.writeFileSync(encryption_path, output, {flag: 'a+'});
        return;
    })
}

function encryptRow(row, configuration_fields, secret_key) {
    try {
        for (let field of configuration_fields) {
            if (row[field.mapped_field] !== undefined && field.encrypted) {
                if (field.type === "date" || field.type === "datetime") {
                    if (row[field.mapped_field] !== null && row[field.mapped_field] !== "" && row[field.mapped_field] !== undefined) {
                        row[field.mapped_field] = crypto.AES.encrypt(utf8.encode(row[field.mapped_field].toString()), secret_key).toString();
                    }
                } else {
                    row[field.mapped_field] = crypto.AES.encrypt(utf8.encode(row[field.mapped_field].toString()), secret_key).toString();
                }
            }
        }
        return row;
    } catch (e) {
        throw e;
    }


}
