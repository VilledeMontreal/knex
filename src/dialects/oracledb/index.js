// Oracledb Client
// -------
const _ = require('lodash');
const inherits = require('inherits');
const QueryCompiler = require('./query/compiler');
const ColumnCompiler = require('./schema/columncompiler');
const BlobHelper = require('./utils').BlobHelper;
const ReturningHelper = require('./utils').ReturningHelper;
const Promise = require('bluebird');
const stream = require('stream');
const Transaction = require('./transaction');
const Client_Oracle = require('../oracle');
const Oracle_Formatter = require('../oracle/formatter');
const debugDialect = require('debug')('knex:oracledb');

function Client_Oracledb() {
  debugDialect('Client_Oracledb()');
  Client_Oracle.apply(this, arguments);
  // Node.js only have 4 background threads by default, oracledb needs one by connection
  if (this.driver) {
    process.env.UV_THREADPOOL_SIZE = process.env.UV_THREADPOOL_SIZE || 1;
    process.env.UV_THREADPOOL_SIZE =
      parseInt(process.env.UV_THREADPOOL_SIZE) + this.driver.poolMax;
  }
}
inherits(Client_Oracledb, Client_Oracle);

Client_Oracledb.prototype.driverName = 'oracledb';

Client_Oracledb.prototype.canDialectManagePool = true;

Client_Oracledb.prototype._driver = function() {
  debugDialect('_driver');
  const client = this;
  const oracledb = require('oracledb');
  client.fetchAsString = [];
  if (this.config.fetchAsString && _.isArray(this.config.fetchAsString)) {
    this.config.fetchAsString.forEach(function(type) {
      if (!_.isString(type)) return;
      type = type.toUpperCase();
      if (oracledb[type]) {
        if (type !== 'NUMBER' && type !== 'DATE' && type !== 'CLOB') {
          this.logger.warn(
            'Only "date", "number" and "clob" are supported for fetchAsString'
          );
        }
        client.fetchAsString.push(oracledb[type]);
      }
    });
  }
  return oracledb;
};

Client_Oracledb.prototype.queryCompiler = function() {
  return new QueryCompiler(this, ...arguments);
};
Client_Oracledb.prototype.columnCompiler = function() {
  return new ColumnCompiler(this, ...arguments);
};
Client_Oracledb.prototype.formatter = function() {
  return new Oracledb_Formatter(this, ...arguments);
};
Client_Oracledb.prototype.transaction = function() {
  return new Transaction(this, ...arguments);
};

Client_Oracledb.prototype.prepBindings = function(bindings) {
  return _.map(bindings, (value) => {
    if (value instanceof BlobHelper && this.driver) {
      return { type: this.driver.BLOB, dir: this.driver.BIND_OUT };
      // Returning helper always use ROWID as string
    } else if (value instanceof ReturningHelper && this.driver) {
      return { type: this.driver.STRING, dir: this.driver.BIND_OUT };
    } else if (typeof value === 'boolean') {
      return value ? 1 : 0;
    }
    return value;
  });
};

// Get a raw connection, called by the `pool` whenever a new
// connection needs to be added to the pool.
Client_Oracledb.prototype.acquireRawConnection = function() {
  debugDialect('acquireRawConnection');
  const client = this;
  const getAsyncConnection = function(driverOrPool) {
    return new Promise(function(resolver, rejecter) {
      // If external authentication dont have to worry about username/password and
      // if not need to set the username and password
      const oracleDbConfig = client.connectionSettings.externalAuth
        ? { externalAuth: client.connectionSettings.externalAuth }
        : {
            user: client.connectionSettings.user,
            password: client.connectionSettings.password,
          };

      if (client.dialectPool && client.connectionSettings.client) {
        oracleDbConfig.user = client.connectionSettings.client.user;
        delete oracleDbConfig.password; // not required since we impersonate user
      }

      // In the case of external authentication connection string will be given
      oracleDbConfig.connectString =
        client.connectionSettings.connectString ||
        client.connectionSettings.host +
          '/' +
          client.connectionSettings.database;

      if (client.connectionSettings.prefetchRowCount) {
        oracleDbConfig.prefetchRows =
          client.connectionSettings.prefetchRowCount;
      }

      if (!_.isUndefined(client.connectionSettings.stmtCacheSize)) {
        oracleDbConfig.stmtCacheSize = client.connectionSettings.stmtCacheSize;
      }

      client.driver.fetchAsString = client.fetchAsString;

      driverOrPool.getConnection(oracleDbConfig, function(err, connection) {
        if (err) {
          return rejecter(err);
        }
        connection.commitAsync = function() {
          return new Promise((commitResolve, commitReject) => {
            if (connection.isTransaction) {
              return commitResolve();
            }
            this.commit(function(err) {
              if (err) {
                return commitReject(err);
              }
              commitResolve();
            });
          });
        };
        connection.rollbackAsync = function() {
          return new Promise((rollbackResolve, rollbackReject) => {
            this.rollback(function(err) {
              if (err) {
                return rollbackReject(err);
              }
              rollbackResolve();
            });
          });
        };
        const fetchAsync = function(sql, bindParams, options, cb) {
          options = options || {};
          options.outFormat = client.driver.OBJECT;
          if (options.resultSet) {
            connection.execute(sql, bindParams || [], options, function(
              err,
              result
            ) {
              if (err) {
                return cb(err);
              }
              const fetchResult = { rows: [], resultSet: result.resultSet };
              const numRows = 100;
              const fetchRowsFromRS = function(connection, resultSet, numRows) {
                resultSet.getRows(numRows, function(err, rows) {
                  if (err) {
                    resultSet.close(function() {
                      return cb(err);
                    });
                  } else if (rows.length === 0) {
                    return cb(null, fetchResult);
                  } else if (rows.length > 0) {
                    if (rows.length === numRows) {
                      fetchResult.rows = fetchResult.rows.concat(rows);
                      fetchRowsFromRS(connection, resultSet, numRows);
                    } else {
                      fetchResult.rows = fetchResult.rows.concat(rows);
                      return cb(null, fetchResult);
                    }
                  }
                });
              };
              fetchRowsFromRS(connection, result.resultSet, numRows);
            });
          } else {
            connection.execute(sql, bindParams || [], options, cb);
          }
        };
        connection.executeAsync = function(sql, bindParams, options) {
          // Read all lob
          return new Promise(function(resultResolve, resultReject) {
            fetchAsync(sql, bindParams, options, function(err, results) {
              if (err) {
                return resultReject(err);
              }
              // Collect LOBs to read
              const lobs = [];
              if (results.rows) {
                if (Array.isArray(results.rows)) {
                  for (let i = 0; i < results.rows.length; i++) {
                    // Iterate through the rows
                    const row = results.rows[i];
                    for (const column in row) {
                      if (row[column] instanceof stream.Readable) {
                        lobs.push({
                          index: i,
                          key: column,
                          stream: row[column],
                        });
                      }
                    }
                  }
                }
              }
              Promise.each(lobs, function(lob) {
                return new Promise(function(lobResolve, lobReject) {
                  readStream(lob.stream, function(err, d) {
                    if (err) {
                      if (results.resultSet) {
                        results.resultSet.close(function() {
                          return lobReject(err);
                        });
                      }
                      return lobReject(err);
                    }
                    results.rows[lob.index][lob.key] = d;
                    lobResolve();
                  });
                });
              }).then(
                function() {
                  if (results.resultSet) {
                    results.resultSet.close(function(err) {
                      if (err) {
                        return resultReject(err);
                      }
                      return resultResolve(results);
                    });
                  }
                  resultResolve(results);
                },
                function(err) {
                  resultReject(err);
                }
              );
            });
          });
        };
        connection.madeWithBuilderOptionClient = client.connectionSettings
          ? client.connectionSettings.client
          : null;
        connection.client = client;

        client.logger.warn('resolved connection');
        resolver(connection);
      });
    });
  };
  // Yield:
  // 1020 passing
  // 3 pending
  //return Promise.resolve(asyncConnection);
  //
  // Yield:
  // 867 passing (25s)
  // 1 pending
  // 1 failing
  // 1) oracle | oracledb
  //      knex.migrate
  //        knex.migrate.latest
  //          should remove the record in the lock table once finished:
  //    AssertionError: expected 1 to be falsy
  //     at /knex/test/integration/migrate/index.js:143:48
  // From previous event:
  //     at Builder.Target.then (lib/interface.js:37:24)
  //     at Context.<anonymous> (test/integration/migrate/index.js:141:12)
  return Promise.resolve()
    .then(() => {
      if (client.dialectPool) {
        client.logger.warn('using memorized oracle pool');
        return client.dialectPool;
      }
      if (client.mustInitDialectPool) {
        client.logger.warn('using oracle pool');
        return client.driver.createPool(client.dialectPoolOptions);
      }
      client.logger.warn('using driver (no oracle pool)');
      return client.driver;
    })
    .then((driverOrPool) => {
      if (client.mustInitDialectPool) {
        client.mustInitDialectPool = false;
        client.dialectPool = driverOrPool;
        client.logger.warn('completed dialectPool init ');
      }
      return getAsyncConnection(driverOrPool);
    });
  //
  // Yield:
  // 1020 passing (38s)
  // 3 pending
  //return asyncConnection;
  //
  // Yield:
  // 867 passing (22s)
  // 1 pending
  // 1 failing
  // 1) oracle | oracledb
  //      knex.migrate
  //        knex.migrate.latest
  //          should remove the record in the lock table once finished:
  //    AssertionError: expected 1 to be falsy
  //return Promise.resolve().then(() => debugDialect('stage1')).then(() => asyncConnection);
};

// Used to explicitly close a connection, called internally by the pool
// when a connection times out or the pool is shutdown.
Client_Oracledb.prototype.destroyRawConnection = function(connection) {
  return connection.release();
};

// Runs the query on the specified connection, providing the bindings
// and any other necessary prep work.
Client_Oracledb.prototype._query = function(connection, obj) {
  return new Promise(function(resolver, rejecter) {
    if (!obj.sql) {
      return rejecter(new Error('The query is empty'));
    }
    const options = { autoCommit: false };
    if (obj.method === 'select') {
      options.resultSet = true;
    }
    connection
      .executeAsync(obj.sql, obj.bindings, options)
      .then(function(response) {
        // Flatten outBinds
        let outBinds = _.flatten(response.outBinds);
        obj.response = response.rows || [];
        obj.rowsAffected = response.rows
          ? response.rows.rowsAffected
          : response.rowsAffected;

        if (obj.method === 'update') {
          const modifiedRowsCount = obj.rowsAffected.length || obj.rowsAffected;
          const updatedObjOutBinding = [];
          const updatedOutBinds = [];
          const updateOutBinds = (i) =>
            function(value, index) {
              const OutBindsOffset = index * modifiedRowsCount;
              updatedOutBinds.push(outBinds[i + OutBindsOffset]);
            };

          for (let i = 0; i < modifiedRowsCount; i++) {
            updatedObjOutBinding.push(obj.outBinding[0]);
            _.each(obj.outBinding[0], updateOutBinds(i));
          }
          outBinds = updatedOutBinds;
          obj.outBinding = updatedObjOutBinding;
        }

        if (!obj.returning && outBinds.length === 0) {
          return connection.commitAsync().then(function() {
            resolver(obj);
          });
        }
        const rowIds = [];
        let offset = 0;
        Promise.each(obj.outBinding, function(ret, line) {
          offset =
            offset +
            (obj.outBinding[line - 1] ? obj.outBinding[line - 1].length : 0);
          return Promise.each(ret, function(out, index) {
            return new Promise(function(bindResolver, bindRejecter) {
              if (out instanceof BlobHelper) {
                const blob = outBinds[index + offset];
                if (out.returning) {
                  obj.response[line] = obj.response[line] || {};
                  obj.response[line][out.columnName] = out.value;
                }
                blob.on('error', function(err) {
                  bindRejecter(err);
                });
                blob.on('finish', function() {
                  bindResolver();
                });
                blob.write(out.value);
                blob.end();
              } else if (obj.outBinding[line][index] === 'ROWID') {
                rowIds.push(outBinds[index + offset]);
                bindResolver();
              } else {
                obj.response[line] = obj.response[line] || {};
                obj.response[line][out] = outBinds[index + offset];
                bindResolver();
              }
            });
          });
        })
          .then(function() {
            return connection.commitAsync();
          })
          .then(function() {
            if (obj.returningSql) {
              return connection
                .executeAsync(obj.returningSql(), rowIds, { resultSet: true })
                .then(function(response) {
                  obj.response = response.rows;
                  return obj;
                }, rejecter);
            }
            return obj;
          }, rejecter)
          .then(function(obj) {
            resolver(obj);
          });
      }, rejecter);
  });
};

// Handle clob
function readStream(stream, cb) {
  const oracledb = require('oracledb');
  let data = '';

  if (stream.iLob.type === oracledb.CLOB) {
    stream.setEncoding('utf-8');
  } else {
    data = Buffer.alloc(0);
  }
  stream.on('error', function(err) {
    cb(err);
  });
  stream.on('data', function(chunk) {
    if (stream.iLob.type === oracledb.CLOB) {
      data += chunk;
    } else {
      data = Buffer.concat([data, chunk]);
    }
  });
  stream.on('end', function() {
    cb(null, data);
  });
}

// Process the response as returned from the query.
Client_Oracledb.prototype.processResponse = function(obj, runner) {
  let response = obj.response;
  const method = obj.method;
  if (obj.output) {
    return obj.output.call(runner, response);
  }
  switch (method) {
    case 'select':
    case 'pluck':
    case 'first':
      if (obj.method === 'pluck') {
        response = _.map(response, obj.pluck);
      }
      return obj.method === 'first' ? response[0] : response;
    case 'insert':
    case 'del':
    case 'update':
    case 'counter':
      if (obj.returning && !_.isEmpty(obj.returning)) {
        if (obj.returning.length === 1 && obj.returning[0] !== '*') {
          return _.flatten(_.map(response, _.values));
        }
        return response;
      } else if (!_.isUndefined(obj.rowsAffected)) {
        return obj.rowsAffected;
      } else {
        return 1;
      }
    default:
      return response;
  }
};

// Required for case using Oracle's poolling instead of tarn one
// See delegatedToDriversDialect in pool config
Client_Oracledb.prototype._dialectAcquireConnectionPool = function(oraclePool) {
  return new Promise.resolve(null);
};

// Required for case using Oracle's poolling instead of tarn one
// See delegatedToDriversDialect in pool config
Client_Oracledb.prototype._dialectInitializePool = function(
  driver,
  knexConfig,
  done
) {
  // in this example we use oracle driver's connection API
  // for that knexConfig is in fact oracleDbConfig and
  // driver is in fact oracledb.
  //
  // We create the pool with an application/service account
  // (which has already been provided in user/password fields of connection config).
  // This is required because we want to impersonate the end-user
  // when doing the driver.getConnection() call (homogenous=false)
  const client = this;
  debugDialect('_dialectInitializePool');
  knexConfig.pool = knexConfig.pool || {};
  knexConfig.pool.options = knexConfig.pool.options || {};
  knexConfig.pool.options.homogeneous = false;
  debugDialect('About to call driver.createPool()');
  return client.driver
    .createPool(knexConfig.pool.options)
    .then((pool) => {
      debugDialect('dialectInitializePool (then)');
      return pool;
    })
    .catch((err) => {
      debugDialect('dialectInitializePool (catch)');
      return err;
    });
};

// Required for case using Oracle's poolling instead of tarn one
// See delegatedToDriversDialect in pool config
//
// All acquired connection must be back before calling this
// See: https://github.com/oracle/node-oracledb/issues/836
Client_Oracledb.prototype._dialectDestroyPool = function(
  driver,
  dialectPool,
  done
) {
  debugDialect('_dialectDestroyPool');
  if (!dialectPool) {
    debugDialect('No pool to destroy!');
    return Promise.resolve();
  }

  return dialectPool
    .close()
    .then(() => debugDialect(`handling ok from dialectPool.close`))
    .catch((err) =>
      debugDialect(
        'handling err from dialectPool.close (%s)',
        JSON.stringify(err)
      )
    );
};

// Required for case using Oracle's poolling instead of tarn one
// See delegatedToDriversDialect in pool config
Client_Oracledb.prototype._dialectReleaseConnectionPool = function(connection) {
  return new Promise.resolve(null);
};

Client_Oracledb.prototype.validateConnection = function(connection) {
  const client = connection.client;
  if (client.dialectPool) {
    return this._dialectValidateConnectionPool(connection, client);
  }
  return true;
};

// Required for case using Oracle's poolling instead of tarn one
// See delegatedToDriversDialect in pool config
Client_Oracledb.prototype._dialectValidateConnectionPool = function(
  connection,
  client
) {
  //const client = this;
  const oldClientUser =
    connection && connection.madeWithBuilderOptionClient
      ? connection.madeWithBuilderOptionClient.user
      : null;
  const newClientUser =
    client && client.connectionSettings && client.connectionSettings.client
      ? client.connectionSettings.client.user
      : null;
  if (
    oldClientUser === newClientUser ||
    (_.isNull(oldClientUser) && _.isNull(newClientUser))
  ) {
    return true;
  }
  client.log.warn(
    `told tarn that this connection is not valid because it's not for the user needed (need ${newClientUser} actually have ${oldClientUser})`
  );
  return false;
};

class Oracledb_Formatter extends Oracle_Formatter {
  // Checks whether a value is a function... if it is, we compile it
  // otherwise we check whether it's a raw
  parameter(value) {
    if (typeof value === 'function') {
      return this.outputQuery(this.compileCallback(value), true);
    } else if (value instanceof BlobHelper) {
      return 'EMPTY_BLOB()';
    }
    return this.unwrapRaw(value, true) || '?';
  }
}

module.exports = Client_Oracledb;
