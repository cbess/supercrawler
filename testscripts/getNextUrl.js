const supercrawler = require("../lib");

const crawler = new supercrawler.Crawler({
  interval: 1000,
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

const startUrl = 'https://perfectgod.com/';

crawler.getUrlList().insertIfNotExists(new supercrawler.Url({
  url: startUrl
})).then(function () {
  return crawler.getUrlList().getNextUrl().then(function () {
    console.log(arguments);
  });
}).then(function () {
  return crawler.getUrlList().getNextUrl().then(function () {
    console.log(arguments);
  });
});
