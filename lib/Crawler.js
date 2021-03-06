const { default: axios, AxiosRequestConfig, AxiosResponse } = require('axios');
let Crawler,
    util = require("util"),
    EventEmitter = require("events").EventEmitter,
    FifoUrlList = require("./FifoUrlList"),
    Url = require("./Url"),
    Promise = require("bluebird"),
    urlMod = require("url"),
    NodeCache = require("node-cache"),
    robotsParser = require("robots-parser"),
    mime = require('mime-types'),
    _ = require("lodash"),
    error = require("./error"),
    DEFAULT_INTERVAL = 1000,
    DEFAULT_CONCURRENT_REQUESTS_LIMIT = 5,
    DEFAULT_ROBOTS_CACHE_TIME = 1000 * 60 * 60,
    DEFAULT_USER_AGENT = "Mozilla/5.0 (compatible; supercrawler/1.0; +https://github.com/brendonboshell/supercrawler)";

/**
 * Object represents an instance of a crawler, i.e. a HTTP client that
 * automatically crawls webpages according to the settings passed to it.
 *
 * @param {Object} [opts] Object of configuration options.
 */
Crawler = function (opts) {
    if (!(this instanceof Crawler)) {
        return new Crawler(opts);
    }

    if (typeof opts === "undefined") {
        opts = {};
    }

    this._urlList = opts.urlList || new FifoUrlList();
    this._interval = opts.interval || DEFAULT_INTERVAL;
    this._concurrentRequestsLimit = opts.concurrentRequestsLimit || DEFAULT_CONCURRENT_REQUESTS_LIMIT;
    this._robotsCache = new NodeCache({
        stdTTL: (opts.robotsCacheTime || DEFAULT_ROBOTS_CACHE_TIME) / 1000
    });
    this._userAgent = opts.userAgent || DEFAULT_USER_AGENT;
    this._request = opts.request || {};
    this._handlers = [];
    this._outstandingRequests = 0;
    this._robotsIgnoreServerError = opts.robotsIgnoreServerError || false;
    this._robotsEnabled = (opts.robotsEnabled !== false);
    this._maxContentLength = opts.maxContentLength || 0;
};

util.inherits(Crawler, EventEmitter);

/**
 * Returns the instance of a \UrlList object that is being used. Unless
 * specified to the constructor, this will be \FifoUrlList type
 *
 * @return {UrlList} Instance of \UrlList type object.
 */
Crawler.prototype.getUrlList = function () {
    return this._urlList;
};

/**
 * Get the interval setting, that is the number of milliseconds that the
 * crawler waits before performing another request.
 *
 * @return {number} Interval in milliseconds.
 */
Crawler.prototype.getInterval = function () {
    let interval = this._interval;
    if (typeof interval === 'function') {
        return interval();
    } else {
        return interval;
    }
};

/** 
 * Sets the interval setting 
 * @param {number|function} interval The msecs of the interval or a function that returns the interval in msecs.
 */
Crawler.prototype.setInterval = function (interval) {
    this._interval = interval;
};

/**
 * Get the maximum number of requests that can be in progress at any one time.
 *
 * @return {number} Maximum number of requests
 */
Crawler.prototype.getConcurrentRequestsLimit = function () {
    return this._concurrentRequestsLimit;
};

/**
 * Get the user agent that is used to make requests.
 *
 * @return {string} User agent
 */
Crawler.prototype.getUserAgent = function (url) {
    if (typeof this._userAgent === 'function') {
        return this._userAgent(url);
    }

    return this._userAgent;
};

/**
 * Custom options to be passed to the axios library.
 *
 * @return {AxiosRequestConfig} Object of request options to be merged with the defaults.
 */
Crawler.prototype.getRequestOptions = function () {
    return this._request;
};

/**
 * Get the maximum content length for the request.
 *
 * @return {number} Max content length in bytes.
 */
Crawler.prototype.getMaxContentLength = function (url) {
    if (typeof this._maxContentLength === 'function') {
        return this._maxContentLength(url);
    }

    return this._maxContentLength;
};

/**
 * Start the crawler. Pages will be crawled according to the configuration
 * provided to the Crawler's constructor.
 *
 * @return {Boolean} True if crawl started; false if crawl already running.
 */
Crawler.prototype.start = function () {
    var concurrentRequestsLimit,
        i;

    // TODO can only start when there are no outstanding requests.

    if (this._started) {
        return false;
    }

    concurrentRequestsLimit = this.getConcurrentRequestsLimit();
    this._started = true;

    for (i = 0; i < concurrentRequestsLimit; i++) {
        this._crawlTick();
    }

    return true;
};

/**
 * Prevent crawling of any further URLs.
 */
Crawler.prototype.stop = function () {
    this._started = false;
};

Crawler.prototype.addHandler = function (contentType, handler) {
    // if this method is called as addHandler(\Function), that means the
    // handler will deal with all content types.
    if (arguments.length === 1) {
        return this.addHandler("*", arguments[0]);
    }

    this._handlers.push({
        contentType: contentType,
        handler: handler
    });

    return true;
};

/**
 * Check if we are allowed to send a request and, if we are, send it. If we
 * are not, reschedule the request for NOW + INTERVAL in the future.
 */
Crawler.prototype._crawlTick = function () {
    var urlList,
        nextRequestDate,
        nowDate,
        self = this;

    // Crawling has stopped, so don't start any new requests
    if (!this._started) {
        return;
    }

    urlList = this.getUrlList();
    nextRequestDate = this._getNextRequestDate();
    nowDate = new Date();

    // Check if we are allowed to send the request yet. If we aren't allowed,
    // schedule the request for LAST_REQUEST_DATE + INTERVAL.
    if (nextRequestDate - nowDate > 0) {
        this._scheduleNextTick();

        return;
    }

    // lastRequestDate must always be set SYNCHRONOUSLY! This is because there
    // will be multiple calls to _crawlTick.
    this._lastRequestDate = nowDate;

    urlList.getNextUrl().then(function (urlObj) {
        var url = urlObj.getUrl();

        // We keep track of number of outstanding requests. If this is >= 1, the
        // queue is still subject to change -> so we do not wish to declare
        // urllistcomplete until those changes are synced with the \UrlList.
        self._outstandingRequests++;

        return self._processUrl(url).then(function (resultUrl) {
            return urlList.upsert(resultUrl);
        }).finally(function () {
            self._outstandingRequests--;
        });
    }).catch(RangeError, function () {
        self.emit("urllistempty");

        if (self._outstandingRequests === 0) {
            self.emit("urllistcomplete");
        }
    }).finally(function () {
        // We must schedule the next check. Note that _scheduleNextTick only ever
        // gets called once and once only PER CALL to _crawlTick.
        self._scheduleNextTick();
    });
};

/**
 * Start the crawl process for a specific URL. This method will first check
 * robots.txt to make sure it allowed to crawl the URL.
 *
 * @param  {string} url   The URL to crawl.
 * @return {Promise}      Promise of result URL object.
 */
Crawler.prototype._processUrl = function (url) {
    const self = this;
    let response;

    const urlList = this.getUrlList();
    this.emit("crawlurl", url);

    // perform url download
    let downloadPromise = null;
    if (this._robotsEnabled) {
        // ignore every robots error, except RobotsNotAllowedError
        downloadPromise = this._downloadAndCheckRobots(url).catch(error.RobotsNotAllowedError, (err) => {
            return Promise.reject(err);
        }).catch(() => { }).then(() => {
            return this._downloadUrl(url, false);
        });
    } else {
        downloadPromise = this._downloadUrl(url, false);
    }

    return downloadPromise.then(function (_response) {
        let contentType,
            location;

        response = _response;
        if (!response) {
            return Promise.reject(new error.RequestError('No response: ' + url));
        }

        contentType = response.headers['content-type'] || mime.lookup(url);
        const statusCode = response.status;
        location = response.headers.location;

        // If this is a redirect, we follow the location header.
        // Otherwise, we get the discovered URLs from the content handlers.
        if (statusCode >= 300 && statusCode < 400) {
            self.emit("redirect", url, location);
            return [urlMod.resolve(url, location)];
        } else {
            return self._fireHandlers(contentType, response.data, url).catch(function (err) {
                self.emit("handlersError", err);
                err = new error.HandlersError("A handlers error occured. " + err.message);

                return Promise.reject(err);
            });
        }
    }).then(function (links) {
        let insertProm;

        self.emit("links", url, links);

        if (typeof urlList.insertIfNotExistsBulk === "undefined") {
            insertProm = Promise.map(links, function (link) {
                return urlList.insertIfNotExists(new Url({
                    url: link
                }));
            });
        } else {
            insertProm = urlList.insertIfNotExistsBulk(links.map(function (link) {
                return new Url({
                    url: link
                });
            }));
        }

        return insertProm;
    }).then(function () {
        return new Url({
            url: url,
            errorCode: null,
            statusCode: response.status
        });
    }).catch(error.RobotsNotAllowedError, function (err) {
        return new Url({
            url: url,
            errorCode: "ROBOTS_NOT_ALLOWED",
            errorMessage: err.message
        });
    }).catch(error.HttpError, function (err) {
        self.emit("httpError", err, url);

        return new Url({
            url: url,
            errorCode: "HTTP_ERROR",
            statusCode: err.statusCode
        });
    }).catch(error.RequestError, function (err) {
        return new Url({
            url: url,
            errorCode: "REQUEST_ERROR",
            errorMessage: err.message
        });
    }).catch(error.HandlersError, function (err) {
        return new Url({
            url: url,
            errorCode: "HANDLERS_ERROR",
            errorMessage: err.message
        });
    }).catch(function (err) {
        return new Url({
            url: url,
            errorCode: "OTHER_ERROR",
            errorMessage: err.message
        });
    }).then(function (url) {
        self.emit("crawledurl", url.getUrl(), url.getErrorCode(), url.getStatusCode(), url.getErrorMessage());

        return url;
    });
};

/**
 * Fire any matching handlers for a particular page that has been crawled.
 *
 * @param  {string} contentType Content type, e.g. "text/html; charset=utf8"
 * @param  {string} body        Body content.
 * @param  {string} url         Page URL, absolute.
 * @return {Promise}            Promise returning an array of discovered links.
 */
Crawler.prototype._fireHandlers = function (contentType, body, url) {
    var ctx;

    contentType = contentType.replace(/;.*$/g, "");

    ctx = {
        body: body,
        url: url,
        contentType: contentType
    };

    return Promise.reduce(this._handlers, function (arr, handlerObj) {
        var handlerContentType = handlerObj.contentType,
            handlerFun = handlerObj.handler,
            match = false;

        if (handlerContentType === "*") {
            match = true;
        } else if (Array.isArray(handlerContentType) && (handlerContentType).indexOf(contentType) > -1) {
            match = true;
        } else if ((contentType + "/").indexOf(handlerContentType + "/") === 0) {
            match = true;
        }

        if (!match) {
            return Promise.resolve(arr);
        }

        return Promise.try(function () {
            return handlerFun(ctx);
        }).then(function (subArr) {
            if (!(subArr instanceof Array)) {
                subArr = [];
            }

            return arr.concat(subArr);
        });
    }, []);
};

/**
 * Download a particular URL. Generally speaking, we do not want to follow
 * redirects, because we just add the destination URLs to the queue and crawl
 * them later. But, when requesting /robots.txt, we do follow the redirects.
 * This is an edge case.
 *
 * @param  {string} url             URL to fetch.
 * @param  {Boolean} followRedirect True if redirect should be followed.
 * @return {Promise}                Promise of result.
 */
Crawler.prototype._downloadUrl = async function (url, followRedirect) {
    const maxContentLength = this.getMaxContentLength(url);
    /** @type {AxiosRequestConfig} */
    const config = _.merge({
        followRedirect: (Boolean(followRedirect) ? 5 : 0),
        responseType: 'arraybuffer',
        maxContentLength: (maxContentLength <= 0 ? undefined : maxContentLength),
        headers: {
            'User-Agent': this.getUserAgent(url)
        },
    }, this.getRequestOptions());

    /** @type {AxiosResponse | null} */
    let response = null;
    try {
        response = await axios.get(url, config);
    } catch (err) {
        throw new error.RequestError(`A request error occurred. ${err.message}`);
    }

    if (!response) {
        throw new error.RequestError('A request error occurred.');
    } else if (response.status >= 400) {
        const err = new error.HttpError(`HTTP status code is ${response.status}`);
        err.statusCode = response.status;

        throw err;
    }

    return response;
};

/**
 * For a specific URL, download the robots.txt file and check the URL against
 * it.
 *
 * @param  {string} url  URL to be checked.
 * @return {Promise}     Promise resolves if allowed, rejects if not allowed.
 */
Crawler.prototype._downloadAndCheckRobots = function (url) {
    const self = this;

    return this._getOrDownloadRobots(url).then(function (robotsTxt) {
        if (!robotsTxt) {
            return;
        }

        const robots = robotsParser(self._getRobotsUrl(url), robotsTxt);
        const isAllowed = robots.isAllowed(url, self.getUserAgent(url));

        if (!isAllowed) {
            return Promise.reject(new error.RobotsNotAllowedError(`The URL ${url} is not allowed to be crawled due to robots.txt exclusion`));
        }
    });
};

/**
 * Fetch the robots.txt file from our cache or, if the cache has expired,
 * send a request to the server to download it.
 *
 * @param  {string} url  URL to get robots.txt for.
 * @return {Promise}     Promise returning the string result of robots.txt.
 */
Crawler.prototype._getOrDownloadRobots = function (url) {
    const self = this;

    // Check if this robots.txt file already exists in the cache.
    const robotsUrl = this._getRobotsUrl(url);
    const robotsTxt = this._robotsCache.get(robotsUrl);
    const ignoreServerError = this._robotsIgnoreServerError;

    if (typeof robotsTxt !== 'undefined') {
        return Promise.resolve(robotsTxt);
    }

    // We want to add /robots.txt to the crawl queue. This is because we may
    // parse the robots.txt file with a content handler, in order to extract
    // it's Sitemap: directives. (And then we'll crawl those sitemaps too!)
    return this.getUrlList().insertIfNotExists(new Url({
        url: robotsUrl
    })).then(function () {
        // robots.txt doesn't exist in the cache, so we have to hit the
        // server to get it.
        return self._downloadUrl(robotsUrl, true);
    }).catch(error.HttpError, function (err) {
        const robotsStatusCode = err.statusCode;

        // if robots returns a dismissable status code, we assume
        // there are no restrictions.
        // 403 is ignored: https://developers.google.com/search/docs/advanced/robots/robots-faq#h1c
        if (robotsStatusCode === 500 && !ignoreServerError) {
            return Promise.reject(new error.RobotsNotAllowedError(
                'No crawling is allowed because robots.txt could not be crawled. Status code ' + robotsStatusCode));
        }

        return Promise.resolve({
            statusCode: 200,
            body: ''
        });
    }).then(function (response) {
        const robotsTxt = response.data.toString();
        self._robotsCache.set(robotsUrl, robotsTxt);

        return robotsTxt;
    });
};

/**
 * Given any URL, find the corresponding URL for the /robots.txt file. Robots
 * files are unique per (host, protcol, port) combination.
 *
 * @param  {string} url  Any URL.
 * @return {string}      URL of robots.txt, e.g. https://example.com/robots.txt
 */
Crawler.prototype._getRobotsUrl = function (url) {
    const parsedUrl = new URL(url);

    // There's a robots for every (host, protocol, port) combination
    const robotsUrl = urlMod.format({
        host: parsedUrl.host,
        protocol: parsedUrl.protocol,
        port: parsedUrl.port || null,
        pathname: "/robots.txt"
    });

    return robotsUrl;
};

/**
 * Get the \Date that we are allowed to send another request. If we haven't
 * already sent a request, this will return the current date.
 *
 * @return {Date} Date of next request.
 */
Crawler.prototype._getNextRequestDate = function () {
    let nextRequestDate;
    const interval = this.getInterval();
    const lastRequestDate = this._lastRequestDate;

    if (!lastRequestDate) {
        nextRequestDate = new Date();
    } else {
        nextRequestDate = new Date(lastRequestDate.getTime() + interval);
    }

    return nextRequestDate;
};

/**
 * Work out when we are allowed to send another request, and schedule a call
 * to _crawlTick.
 */
Crawler.prototype._scheduleNextTick = function () {
    const self = this;
    const nextRequestDate = this._getNextRequestDate();
    const nowDate = new Date();
    const delayMs = Math.max(0, nextRequestDate - nowDate);

    setTimeout(function () {
        self._crawlTick();
    }, delayMs);
};

module.exports = Crawler;
