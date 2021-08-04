'use strict';

function getMinNumber(value, min) {
    if (Number(value) < min) {
        return Number(value);
    }
    return min;
}

function getMaxNumber(value, max) {

    if (Number(value) > max) {
        return Number(value);
    }
    return max;
}

function getMinDate(date, min) {
    if (date === undefined || date.toString() === "Invalid date" || date === "" || date === "NaT") {
        return min;
    }
    if (new Date(date).toISOString().split('.')[0] + "Z" < min) {
        return new Date(date).toISOString().split('.')[0] + "Z";
    }
    return min;
}

function getMaxDate(date, max) {
    if (date === undefined || date.toString() === "Invalid date" || date === "" || date === "NaT") {
        return max;
    }

    if (new Date(date).toISOString().split('.')[0] + "Z" > max) {
        return new Date(date).toISOString().split('.')[0] + "Z";
    }
    return max;
}

function secretPassword() {
    var chars = [
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ", // letters
        "0123456789", // numbers
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
    ];
    return [1, 1, 30].map(function (len, i) {
        return Array(len).fill(chars[i]).map(function (x) {
            return x[Math.floor(Math.random() * x.length)];
        }).join('');
    }).concat().join('').split('').sort(function () {
        return 0.5 - Math.random();
    }).join('')
}

module.exports.getMinNumber = getMinNumber;
module.exports.getMaxNumber = getMaxNumber;
module.exports.getMinDate = getMinDate;
module.exports.getMaxDate = getMaxDate;
module.exports.secretPassword = secretPassword;
