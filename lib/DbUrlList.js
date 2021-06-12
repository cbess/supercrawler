const Promise = require('bluebird');
const Url = require('./Url');
const Sequelize = require('sequelize');
const crypto = require('crypto');
const sha1 = crypto.createHash('sha1');

/**
 * A database backed queue. Generates URLs that:
 * (a) has not been crawled and is not being crawled (errorCode == null && statusCode == null); OR
 * (b) a crawl that failed. (errorCode !== NULL)
 * 
 * Provide database details in opts.db. Database connection managed by Sequelize.
 *
 * @param {Object} opts Options
 */
const DbUrlList = function (opts) {
    if (!(this instanceof DbUrlList)) {
        return new DbUrlList(opts);
    }

    if (!opts) {
        opts = {};
    }

    if (typeof opts.db === 'undefined') {
        throw new Error('Must provide db options');
    }

    // Some options defaults
    if (!opts.db.table) {
        opts.db.table = 'url';
    }

    if (opts.db.sequelizeOpts) {
        opts.db.sequelizeOpts.logging = false;
    }

    this._db = new Sequelize(opts.db.database, opts.db.username, opts.db.password, opts.db.sequelizeOpts);
    // create/define URL model
    // ref: https://sequelize.org/v5/class/lib/model.js~Model.html
    this._urlTable = this._db.define(opts.db.table, {
        urlHash: {
            type: Sequelize.STRING(128),
            allowNull: false
        },
        url: {
            type: Sequelize.STRING(10000),
            allowNull: false
        },
        statusCode: {
            type: Sequelize.STRING,
            allowNull: true
        },
        errorCode: {
            type: Sequelize.STRING,
            allowNull: true
        },
        errorMessage: {
            type: Sequelize.STRING(1000),
            allowNull: true
        },
        numErrors: {
            type: Sequelize.INTEGER(10),
            allowNull: false
        },
        crawled: {
            type: Sequelize.BOOLEAN,
            allowNull: false,
        }
    }, {
        indexes: [{
            // used to find matches for various model SQL methods (e.g. upsert)
            unique: true,
            fields: ['urlHash']
        }, {
            unique: false,
            fields: ['crawled']
        }]
    });
    this._urlTableSynced = false;
    this._initialRetryTime = 1000 * 60 * 60;
};

/**
 * Create the URL table if it doesn't already exist, and return it (promise).
 *
 * @return {Promise} Promise returning the Sequelize URL table model.
 */
DbUrlList.prototype._getUrlTable = function () {
    const self = this;
    let syncProm = Promise.resolve();

    if (!this._urlTableSynced) {
        syncProm = this._urlTable.sync();
    }

    this._urlTableSynced = true;

    return syncProm.then(function () {
        return self._urlTable;
    });
};

/**
 * Insert new URL record if it doesn't already exist. If it does exist, this
 * function resolves anyway.
 *
 * @param  {Url} url    Url object
 * @return {Promise}    Promise resolved when record inserted.
 */
DbUrlList.prototype.insertIfNotExists = function (url) {
    const self = this;

    return this._getUrlTable().then(function (urlTable) {
        return urlTable.create(self._makeUrlRow(url)).catch(Sequelize.UniqueConstraintError, function () {
            // we ignore unqiue constraint errors
            return true;
        });
    });
};

/**
 * A method to insert an array of URLs in bulk. This is useful when we are
 * trying to insert 50,000 URLs discovered in a sitemaps file, for example.
 *
 * @param  {Array} urls  Array of URL objects to insert.
 * @return {Promise}     Promise resolves when everything is inserted.
 */
DbUrlList.prototype.insertIfNotExistsBulk = function (urls) {
    const self = this;

    return this._getUrlTable().then(function (urlTable) {
        return urlTable.bulkCreate(urls.map(function (url) {
            return self._makeUrlRow(url);
        }), {
            ignoreDuplicates: true
        });
    });
};

/**
 * Given a URL object, create the corresponding row to be inserted into the
 * urls table.
 *
 * @param  {Url} url    Url object.
 * @return {Object}     Row to be inserted into the url table.
 */
DbUrlList.prototype._makeUrlRow = function (url) {
    const urlHash = sha1.copy().update(url.getUrl()).digest('hex');

    return {
        urlHash: urlHash, // like an alias for primary key
        url: url.getUrl(),
        statusCode: url.getStatusCode(),
        errorCode: url.getErrorCode(),
        errorMessage: url.getErrorMessage(),
        numErrors: url.getErrorCode() === null ? 0 : 1,
    };
};

/**
 * Insert a record, or update it if it already exists.
 *
 * @param  {Url} url    Url object.
 * @return {Promise}    Promise resolved once record upserted.
 */
DbUrlList.prototype.upsert = function (url) {
    const urlHash = sha1.copy().update(url.getUrl()).digest('hex');

    return this._getUrlTable().then(function (urlTable) {
        var findProm;

        // if there's an error, we must get the existing URL record first, so we
        // can increment the error count.
        if (url.getErrorCode() === null) {
            findProm = Promise.resolve(null);
        } else {
            findProm = urlTable.findOne({
                where: {
                    urlHash: urlHash
                }
            });
        }

        return findProm.then(function (record) {
            let numErrors = 0;

            if (record !== null) {
                numErrors = record.get('numErrors');
            }

            return urlTable.upsert({
                urlHash: urlHash, // unique primary key
                url: url.getUrl(),
                statusCode: url.getStatusCode(),
                errorCode: url.getErrorCode(),
                errorMessage: url.getErrorMessage(),
                numErrors: url.getErrorCode() === null ? 0 : (numErrors + 1),
            });
        });
    });
};

/**
 * Get the next URL to be crawled.
 *
 * @return {Promise} Promise resolving with the next URL to be crawled.
 */
DbUrlList.prototype.getNextUrl = function () {
    const self = this;

    return this._getUrlTable().then(function (urlTable) {
        return urlTable.findOne({
            where: {
                crawled: false,
            },
            // get next URL based on ID sequence
            order: ['id', 'ASC'],
        }).then(function (urlRecord) {
            if (urlRecord === null) {
                return Promise.reject(new RangeError('The URL list has been exhausted.'));
            }

            // update the recrod that will be next, to no longer be "next"
            return urlTable.update({ crawled: true }, {
                where: {
                    id: urlRecord.get('id')
                }
            }).then(function (res) {
                const numAffected = res[0];

                // If we haven't managed to update this record, that means another
                // process has updated it! So we'll have to try again
                if (numAffected === 0) {
                    return self.getNextUrl();
                }

                // We've managed to secure this URL for our process to crawl.
                return new Url({
                    url: urlRecord.get('url'),
                    statusCode: urlRecord.get('statusCode'),
                    errorCode: urlRecord.get('errorCode'),
                    errorMessage: urlRecord.get('errorMessage')
                });
            });
        });
    });
};

module.exports = DbUrlList;
