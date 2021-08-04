'use strict';

const axios = require('axios');

function sendKey(url, secretKey, id, token, userToken) {
    let body = {
        id: id,
        key: secretKey
    };
    return axios({
        method: "post",
        url: url,
        headers: {
            "Content-Type": "application/json",
            "Token": token,
            "Authorization": userToken
        },
        data: JSON.stringify(body)
    });
}

function getKey(url, token, userToken) {

    return axios({
        method: "get",
        url: url,
        headers: {
            "Token": token,
            "Authorization": userToken
        }
    });
}

module.exports.sendKey = sendKey;
module.exports.getKey = getKey;
