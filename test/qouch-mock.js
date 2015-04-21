var Q = require('q');
var fs = require('q-io/fs');
var crypto = require('crypto');

module.exports = qouchMockFactory;

function qouchMockFactory ( docs, designDocPaths ) {

  var designDocsCache = [];
  var updateSeq = 0;
  var docCache = docs.reduce(function ( ret, doc ) {
    updateIdRev(doc);
    ret[ doc._id ] = doc;
    return ret;
  }, {});

  QouchMock.QouchRequestError = QouchRequestError;
  QouchMock.QouchBulkError = QouchBulkError;

  function QouchMock() {}

  QouchMock.prototype.createDB = function () {
    throw new Error('Method not mocked');
  };

  QouchMock.prototype.deleteDB = function () {
    throw new Error('Method not mocked');
  };

  QouchMock.prototype.seq = function () {
    return Q.when(updateSeq);
  };

  QouchMock.prototype.get = function ( _id ) {
    if ( !docCache[ _id ] ) {
      return Q.when(
          new Error('404 Doc Not Found')
      );
    }
    return Q.when(
        JSON.parse(JSON.stringify(docCache[ _id ]))
    );
  };

  QouchMock.prototype.fetch = function ( _ids ) {
    _ids = _ids || [];
    return Q.when(
        Object.keys(docCache)
        .filter(function ( _id ) {
          return !!~_ids.indexOf(_id);
        })
        .map(function ( _id ) {
          return docCache[ _id ];
        })
    );
  };

  QouchMock.prototype.fetchAll = function () {
    return this.fetch();
  };

  QouchMock.prototype.insert = function ( doc ) {
    updateIdRev(doc);
    docCache[ doc._id ] = doc;
    updateSeq++;
    return Q.when(
      { _id: doc._id, _rev: doc._rev }
    );
  };

  QouchMock.prototype.update = function ( doc ) {
    updateSeq++;
    return this.insert(doc)
    .then(function ( info ) {
      delete info._id;
      return info;
    });
  };

  QouchMock.prototype.destroy = function ( doc ) {
    throw new Error('Method not mocked');
  };

  QouchMock.prototype.bulk = function ( docs ) {
    return Q.all(
        docs.map(this.insert.bind(this))
    );
  };

  QouchMock.prototype.view = function ( design, view, params ) {
    return Q.fcall(function () {
      if ( !designDocsCache.length ) {
        return Q.all(
            designDocPaths.map(function ( docPath ) {
              return fs.read(docPath)
              .then(function ( content ) {
                return eval('( function () { return ' + content + '} )()');
              })
              .fail(function ( e ) {
                e.message = '(' + docPath + ')\t' + e.message;
                throw e;
              });
            })
        )
        .then(function ( designDocs ) {
          designDocsCache = designDocs;
        });
      }
      return designDocsCache;
    })
    .then(function () {
      var designDocsThatMatch = designDocsCache.filter(function ( designDoc ) {
        return designDoc._id === '_design/' + design;
      });
      if ( designDocsThatMatch.length !== 1 ) {
        throw new Error('More than one design doc with the name: ' + design);
      }
      var designDoc = designDocsThatMatch[ 0 ];
      var viewCode = designDoc.views[ view ];

      var rows = [];
      function couchMap ( docsToMap ) {
        var doc = docsToMap.shift();
        function emit( key, refDoc ) {
          var docToSend;
          if ( params.include_docs ) {
            if ( refDoc && refDoc._id ) {
              docToSend = docCache[ refDoc._id ];
            } else {
              docToSend = doc;
            }
          } else {
            docToSend = undefined;
          }
          rows.push({
            key: key,
            id: doc._id,
            value: {
              _rev: doc._rev
            },
            doc: docToSend
          });
        }
        var mapFn = eval('(' + viewCode.map + ')');
        mapFn(doc);
        if ( docsToMap.length > 0 ) {
          couchMap(docsToMap);
        }
      }

      var allDocs = Object.keys(docCache).map(function ( _id ) {
        return docCache[ _id ];
      });

      couchMap(allDocs);
      var matchedRows;

      function matchKey( baseKey, keyToMatch ) {
        if ( Array.isArray(keyToMatch) && Array.isArray(baseKey) ) {
          return baseKey.every(function ( rowKey, ix ) {
            return keyToMatch[ix] && rowKey === keyToMatch[ix];
          });
        } else {
          return keyToMatch === baseKey;
        }
      }

      if ( params.keys ) {
        matchedRows = rows.filter(function ( row ) {
          return params.keys.some(function ( key ) {
            return matchKey(row.key, key);
          });
        });
      } else if ( params.key ) {
        matchedRows = rows.filter(function ( row ) {
          return matchKey(row.key, params.key);
        });
      } else if ( params.rootKey ) {
        matchedRows = rows.filter(function ( row ) {
          return matchKey(params.rootKey, row.key);
        });
      } else if ( params.startkey || params.start_key || params.endkey || params.end_key ) {
        var startKey = params.startkey ? params.startkey : params.start_key;
        var endKey = params.endkey ? params.endkey : params.end_key;

        rows.sort(function ( row1, row2 ) {
          if ( !Array.isArray(row1.key) ) {
            if ( row1.key < row2.key ) {
              return 1;
            }
            if ( row1.key > row2.key ) {
              return -1;
            }
            return 0;
          }

          for ( var i = 0; i < row1.key.length; i++ ) {
            if ( row1.key[ i ] < row2.key[ i ] ) {
              return -1;
            }
            if ( row1.key[ i ] > row2.key[ i ] ) {
              return 1;
            }
          }
          return 0;
        });

        matchedRows = rows.filter(function ( row ) {
          if ( !Array.isArray(row.key) ) {
            return row.key >= params.start_key && row.key <= params.end_key;
          }

          for ( var i = 0; i < row.key.length; i++ ) {
            if (
              i < startKey.length &&
              !( row.key[ i ] >= startKey[ i ] && row.key[ i ] <= endKey[ i ] )
            ) {
              return false;
            }
          }
          return true;
        });
      }
      if ( !params.reduce || !matchedRows ) {
        return matchedRows || [];
      }

      var reduceFn = eval('(' + viewCode.reduce + ')');

      function sum( array ) {
        return array.reduce(function ( sum, element ) {
          return sum + element;
        }, 0);
      }

      function count( array ) {
        throw new Error('Not Implemented');
      }

      function stats( array ) {
        throw new Error('Not Implemented');
      }

      var keys = matchedRows.map(function ( matchedRow ) {
        return [ matchedRows.key, matchedRows.id ];
      });

      var values = matchedRows.map(function ( matchedRow ) {
        return matchedRow.doc;
      });

      var firstReducedValues = reduceFn(keys.slice(0, matchedRows.length / 2), values.slice(0, matchedRows.length / 2), false);
      var secondReducedValues = reduceFn(keys.slice(matchedRows.length / 2, matchedRows.length), matchedRows.slice(matchedRows.length / 2, matchedRows.length), false);
      var finalReducedValue =  reduceFn(null, [ firstReducedValues, secondReducedValues ], true);
      return [ { value: finalReducedValue } ];
    });

  };

  QouchMock.prototype.viewDocs = function ( design, view, params ) {
    if (!params) params = {};
    params.reduce = false;
    params.include_docs = true;

    return this.view(design, view, params)
    .then(function (rows) {
      return rows.map(function (row) {
        return row.doc;
      });
    });
  };

  QouchMock.prototype.request = function ( method, path, body ) {
    throw new Error('Method not mocked');
  };

  return QouchMock;
}

function updateIdRev ( doc ) {
  if ( !doc._id ) {
    doc._id = UUID();
  }
  if ( !doc._rev ) {
    doc._rev = UUID() + '-' + 1;
  } else {
    doc._rev = doc._rev.replace(/\-(\d+$)/, function ( match, rev ) {
      return '-' + (Number(rev) + 1) ;
    });
  }
}

function UUID () {
  return crypto.randomBytes(16).toString('hex');
}

function QouchRequestError ( message, statusCode, requestOptions, response) {
  this.message = message;
  this.httpStatusCode = statusCode;
  this.requestOptions = requestOptions;
  this.response = response;
}
QouchRequestError.prototype = new Error();
QouchRequestError.prototype.constructor = QouchRequestError;

function QouchBulkError ( dbURL, itemErrors, requestBody ) {
  this.message = 'Bulk Errors';
  this.dbURL = dbURL;
  this.itemErrors = itemErrors;
  this.requestBody = requestBody;
}
QouchBulkError.prototype = new Error();
QouchBulkError.prototype.constructor = QouchBulkError;
