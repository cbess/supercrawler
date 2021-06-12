/* globals console */
const supercrawler = require('../lib');

const crawler = new supercrawler.Crawler({
  interval: 100,
  concurrentRequestsLimit: 5,
  urlList: new supercrawler.DbUrlList({
    db: {
      sequelizeOpts: {
        dialect: 'sqlite',
        storage: 'tmp.sqlite',
      }
    }
  })
});

const hostname = 'perfectgod.com';

crawler.on('crawlurl', function (url) {
  console.log('Crawling ' + url);
});
crawler.on('urllistempty', function () {
  console.warn('The URL queue is empty.');
});
crawler.on('urllistcomplete', function () {
  console.warn('Crawling done.');
  crawler.stop();
});
crawler.on('handlersError', function (err) {
  console.error(err);
});
crawler.addHandler('text/html', supercrawler.handlers.htmlLinkParser({
  hostnames: [hostname]
}));
crawler.addHandler(function (context) {
  console.log('Processed ' + context.url);
});

crawler.getUrlList().insertIfNotExists(new supercrawler.Url({
  url: `https://${hostname}`,
})).then(function () {
  crawler.start();
});
