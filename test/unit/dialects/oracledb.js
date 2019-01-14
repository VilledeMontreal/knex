// global it, describe, expect

'use strict';
var _ = require('lodash');
var expect = require('chai').expect;
var knex = require('../../../knex');
var config = require('../../knexfile');
var configDialectPool4Proxy;
var configDialectPool1Account;
const debug = require('debug')('knex:oracledb');

beforeEach(function() {
  // Init here to avoid side effect if tampered in a test
  configDialectPool4Proxy = _.cloneDeep(config.oracledb);
  configDialectPool4Proxy.pool = {
    delegateToDriverDialect: true,
    min: 1,
    max: 1,
    options: {
      homogeneous: false,
      user: config.oracledb.connection.user,
      password: config.oracledb.connection.password,
      connectString: config.oracledb.connection.connectString,
      stmtCacheSize: 0,
    },
  };
  configDialectPool1Account = _.cloneDeep(config.oracledb);
  configDialectPool1Account.pool = {
    delegateToDriverDialect: true,
    min: 1,
    max: 1,
    options: {
      homogeneous: true,
      user: config.oracledb.connection.user,
      password: config.oracledb.connection.password,
      connectString: config.oracledb.connection.connectString,
      stmtCacheSize: 0,
    },
  };
});

describe('OracleDb externalAuth', function() {
  var knexInstance;
  var spy;
  var conf;

  before(function() {
    conf = _.cloneDeep(config.oracledb);
    conf.connection.externalAuth = true;
    conf.connection.host = 'host';
    conf.connection.database = 'database';
    knexInstance = knex(conf);
    spy = sinon.spy(knexInstance.client.driver, 'getConnection');
    return knexInstance;
  });

  it('externalAuth and connectString should be sent to the getConnection', function() {
    var connectionWithExternalAuth = {
      connectString: conf.connection.connectString,
      externalAuth: true,
    };

    function expectCallWithGoodParams() {
      expect(spy).to.have.callCount(1);
      expect(spy).to.have.been.calledWith(connectionWithExternalAuth);
    }

    return knexInstance.client.acquireRawConnection().then(
      function(resolve) {
        expectCallWithGoodParams;
      },
      function(reject) {
        debug(`rejected with ${JSON.stringify(reject)}`);
        expectCallWithGoodParams;
      }
    );
  });

  after(function() {
    knexInstance.client.driver.getConnection.restore();
  });
});

describe('OracleDb proxy connection', function() {
  var knexInstance;
  var spy;

  before(function() {
    knexInstance = knex(configDialectPool4Proxy);
    spy = sinon.spy(knexInstance.client, 'acquireConnection');
  });

  it(`builder options should be sent to acquireConnection`, function() {
    var clientInfo = {
      user: config.oracledb.connection.enduser, // the session user
    };
    const expectCallWithGoodParams = function() {
      expect(spy).to.have.callCount(1);
      expect(spy).to.have.been.calledWith(clientInfo);
    };
    return knexInstance('DUAL')
      .options({ client: clientInfo })
      .then(
        function(resolve) {
          expectCallWithGoodParams();
        },
        function(reject) {
          debug(`rejected with ${JSON.stringify(reject)}`);
          expectCallWithGoodParams();
        }
      );
  });

  after(function() {
    knexInstance.client.acquireConnection.restore();
  });
});

describe('OracleDb mix of proxy/non-proxy request', function() {
  var knexInstance;
  var spy;

  before(function() {
    knexInstance = knex(configDialectPool4Proxy);
    spy = sinon.spy(knexInstance.client, 'acquireConnection');
  });

  it(`builder options should not affect subsequent call`, function() {
    var clientInfoFirstCall = {
      user: config.oracledb.connection.enduser, // the session user
    };
    var clientInfoThirdCall = {
      user: config.oracledb.connection.otherenduser, // the session user
    };
    const expectCallWithGoodParams = function() {
      expect(spy).to.have.callCount(3);

      expect(spy.firstCall.args).to.not.equal(spy.secondCall.args);
      expect(spy.firstCall.args).to.not.equal(spy.thirdCall.args);
      expect(spy.secondCall.args).to.not.equal(spy.thirdCall.args);
    };
    return knexInstance('DUAL')
      .options({ client: clientInfoFirstCall })
      .then(function(resolve) {
        return knexInstance('DUAL');
      })
      .then(function(resolve) {
        return knexInstance('DUAL').options({ client: clientInfoThirdCall });
      })
      .then(
        function(resolve) {
          expectCallWithGoodParams();
        },
        function(reject) {
          debug(`rejected with ${JSON.stringify(reject)}`);
          expectCallWithGoodParams();
        }
      );
  });

  it('allows you to select with impersonated user', function() {
    const rows = [];
    return knexInstance('DUAL')
      .select(knexInstance.raw('USER'))
      .options({ client: { user: config.oracledb.connection.otherenduser } })
      .stream(function(rowStream) {
        rowStream.on('data', function(row) {
          rows.push(row);
        });
      })
      .then(function() {
        expect(rows).to.have.lengthOf(1);
        expect(rows[0][0]).to.equal(
          config.oracledb.connection.otherenduser.toUpperCase()
        );
      });
  });

  after(function() {
    knexInstance.client.acquireConnection.restore();
  });
});

describe('OracleDb request for transactions', function() {
  var knexInstance;

  before(function() {
    knexInstance = knex(configDialectPool4Proxy);
  });

  it(`Generate transaction`, function() {
    return knexInstance.transaction(function(trx) {
      return trx('DUAL')
        .select('*')
        .then(function(resp) {
          expect(resp).to.have.lengthOf(1);
        });
    });
  });

  it(`Generate transaction with proxy user in options with fail result`, function() {
    return knexInstance.transaction(function(trx) {
      return trx('DUAL')
        .select(trx.raw('USER'))
        .options({ client: { user: config.oracledb.connection.otherenduser } })
        .then(function(resp) {
          expect(resp).to.have.lengthOf(1);
          expect(resp[0]['USER']).to.not.equal(
            config.oracledb.connection.otherenduser.toUpperCase()
          );
        });
    });
  });

  it(`Generate transaction with proxy user in transactions`, function() {
    return knexInstance.transaction(
      function(trx) {
        return trx('DUAL')
          .select(trx.raw('USER'))
          .then(function(resp) {
            expect(resp).to.have.lengthOf(1);
            expect(resp[0]['USER']).to.be.equal(
              config.oracledb.connection.otherenduser.toUpperCase()
            );
          });
      },
      { client: { user: config.oracledb.connection.otherenduser } }
    );
  });

  after(function() {
    knexInstance.destroy();
  });
});

describe('OracleDb parameters', function() {
  describe('with fetchAsString parameter', function() {
    var knexClient;

    before(function() {
      var conf = _.clone(config.oracledb);
      conf.fetchAsString = ['number', 'DATE', 'cLOb'];
      knexClient = knex(conf);
      return knexClient;
    });

    it('on float', function() {
      return knexClient
        .raw('select 7.329 as "field" from dual')
        .then(function(result) {
          expect(result[0]).to.be.ok;
          expect(result[0].field).to.be.a('string');
        });
    });

    it('on date', function() {
      return knexClient
        .raw('select CURRENT_DATE as "field" from dual')
        .then(function(result) {
          expect(result[0]).to.be.ok;
          expect(result[0].field).to.be.a('string');
        });
    });

    after(function() {
      return knexClient.destroy();
    });
  });

  describe('without fetchAsString parameter', function() {
    var knexClient;

    before(function() {
      knexClient = knex(config.oracledb);
      return knexClient;
    });

    it('on float', function() {
      return knexClient
        .raw('select 7.329 as "field" from dual')
        .then(function(result) {
          expect(result[0]).to.be.ok;
          expect(result[0].field).to.not.be.a('string');
        });
    });

    it('on date', function() {
      return knexClient
        .raw('select CURRENT_DATE as "field" from dual')
        .then(function(result) {
          expect(result[0]).to.be.ok;
          expect(result[0].field).to.not.be.a('string');
        });
    });

    after(function() {
      return knexClient.destroy();
    });
  });

  describe("OracleDb driver's pool", function() {
    var knexInstance;
    var spy;
    var connection;

    before(function() {
      knexInstance = knex(configDialectPool1Account);
      spy = sinon.spy(knexInstance.client.driver, 'createPool');
      return knexInstance;
    });

    it('creates oracle pool when delegateToDriverDialect is true', function() {
      var poolWithHeterogeneous = {
        homogeneous: true,
        user: config.oracledb.connection.user,
        password: config.oracledb.connection.password,
        connectString: config.oracledb.connection.connectString,
        stmtCacheSize: 0,
      };
      const expectCallWithGoodParams = function() {
        expect(spy).to.have.callCount(1);
        expect(spy).to.have.been.calledWith(poolWithHeterogeneous);
      };
      return knexInstance('DUAL').then(
        function(resolve) {
          connection = resolve;
          expectCallWithGoodParams();
        },
        function(reject) {
          debug(`rejected with ${JSON.stringify(reject)}`);
          expectCallWithGoodParams();
        }
      );
    });

    after(function() {
      var prom;

      knexInstance.client.driver.createPool.restore();

      if (connection) {
        prom = knexInstance.client.destroyRawConnection(connection);
      } else {
        prom = Promise.resolve();
      }
      return prom.then(() => knexInstance.destroy());
    });
  });
});
