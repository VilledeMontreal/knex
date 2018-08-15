// global it, describe, expect

'use strict';
var _ = require('lodash');
var expect = require('chai').expect;
var knex = require('../../../knex');
var config = require('../../knexfile');

describe('OracleDb externalAuth', function() {
  var knexInstance = knex({
    client: 'oracledb',
    connection: {
      user: 'user',
      password: 'password',
      connectString: 'connect-string',
      externalAuth: true,
      host: 'host',
      database: 'database',
    },
  });
  var spy;

  before(function() {
    spy = sinon.spy(knexInstance.client.driver, 'getConnection');
  });

  it('externalAuth and connectString should be sent to the getConnection', function() {
    var connectionWithExternalAuth = {
      connectString: 'connect-string',
      externalAuth: true,
    };
    knexInstance.client
      .acquireRawConnection()
      .then(function(resolve) {}, function(reject) {});
    expect(spy).to.have.callCount(1);
    expect(spy).to.have.been.calledWith(connectionWithExternalAuth);
  });

  after(function() {
    knexInstance.client.driver.getConnection.restore();
  });
});

describe.skip('OracleDb proxy connection', function() {
  var knexInstance = knex({
    client: 'oracledb',
    connection: {
      user: 'user',
      password: 'password',
      connectString: 'connect-string',
      host: 'host',
      database: 'database',
      delegateToDriverDialect: true, // required for proxy
    },
  });
  var spy;

  before(function() {
    spy = sinon.spy(knexInstance.client.driver.Pool.prototype, 'getConnection');
  });

  it(`honoring .options({ user: 'enduser' }) syntax`, function() {
    var endUser = {
      user: 'enduser', // the session user
    };
    knexInstance('dual')
      .options({ user: 'enduser' })
      .client.acquireRawConnection()
      .then(function(resolve) {}, function(reject) {});
    expect(spy).to.have.callCount(1);
    expect(spy).to.have.been.calledWith(endUser);
  });

  after(function() {
    knexInstance.client.driver.Pool.prototype.getConnection.restore();
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
      console.log('in with fetchAsString parameter (after)');
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
      knexInstance = knex({
        client: 'oracledb',
        connection: {
          user: 'service',
          password: 'account',
          connectString: 'connect-string',
          host: 'host',
          database: 'database',
        },
        pool: {
          delegateToDriverDialect: true,
          max: 1, // Required so to create the pool
          min: 1, // To avoid Error: Tarn: opt.max is smaller than opt.min
        },
      });
      spy = sinon.spy(knexInstance.client.driver, 'createPool');
      return knexInstance;
    });

    it('bypass knex pool when delegateToDriverDialect is true', function() {
      var poolWithHeterogeneous = {
        user: 'service',
        password: 'account',
        connectString: 'connect-string',
        homogeneous: false,
      };
      return knexInstance.client.acquireRawConnection().then(
        function(resolve) {
          connection = resolve;
          expect(spy).to.have.callCount(1);
          expect(spy).to.have.been.calledWith(poolWithHeterogeneous);
        },
        function(reject) {}
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
