var URL = require('url');
var util = require('util');
var Q = require('q');
var http = require('q-io/http');

module.exports = Qouch;
Qouch.QouchRequestError = QouchRequestError;
Qouch.QouchBulkError = QouchBulkError;

function Qouch(url) {
  this.url = url;
}

Qouch.prototype.createDB = function() {
  return this.request('PUT', null, {});
};

Qouch.prototype.deleteDB = function() {
  return this.request('DELETE', null, {});
};

Qouch.prototype.seq = function(_id) {
  return http.read(this.url)
  .then(function(body) {
    return JSON.parse(body).update_seq;
  });
};

Qouch.prototype.get = function(_id) {
  return http.read(util.format('%s/%s', this.url, _id))
  .then(function(body) {
    return JSON.parse(body);
  });
};

Qouch.prototype.fetch = function(_ids) {
  return this.request('POST', '_all_docs?include_docs=true', { keys: _ids || [] })
  .then(function(body) {
    return body.rows.map(function(row) { return row.doc; });
  });
};

Qouch.prototype.fetchAll = function() {
  return this.request('GET', '_all_docs?include_docs=true')
  .then(function(body) {
    return body.rows.map(function(row) { return row.doc; });
  });
};

Qouch.prototype.insert = function(doc) {
  return this.request('POST', null, doc)
  .then(function(body) {
    return { _id: body.id, _rev: body.rev };
  });
};

Qouch.prototype.update = function(doc) {
  return this.request('PUT', doc._id, doc)
  .then(function(body) {
    return { _rev: body.rev };
  });
};

Qouch.prototype.destroy = function(doc) {
  var clone = JSON.parse(JSON.stringify(doc));
  clone._deleted = true;
  
  return this.request('PUT', doc._id, clone)
  .then(function(body) {
    return { _rev: body.rev, _deleted: true };
  });
};

Qouch.prototype.bulk = function(docs) {
  var dbURL = this.url;

  return this.request('POST', '_bulk_docs', { docs: docs })
  .then(function(body) {
    var errors = body.filter(function(item) {
      return typeof item.error != 'undefined';
    })
    .map(function ( item ) {
      return { _id: item.id, error: item.error };
    });

    if ( errors.length ) {
      throw new QouchBulkError(dbURL, errors, docs);
    }

    return body.map(function(item) {
      return { _id: item.id, _rev: item.rev };
    });
  });
};

Qouch.prototype.view = function(design, view, params) {
  var method;
  var body;
  var qs = '';

  if (params) {
    if (params.keys) {
      method = 'POST';
      body = { keys: params.keys };
      delete params.keys;
    } else if (params.rootKey) {
      params.startkey = params.rootKey;
      params.endkey = params.rootKey.concat({});
      delete params.rootKey;
    }

    Object.keys(params).forEach(function(key) {
      qs += ( qs.length ? '&' : '?') + key + '=' + encodeURIComponent(JSON.stringify(params[key]) );
    });
  }

  if (!method) method = 'GET';

  var path = util.format('_design/%s/_view/%s%s', design, view, qs);

  return this.request(method, path, body)
  .then(function(body) {
    return body.rows;
  });
};

Qouch.prototype.viewDocs = function(design, view, params) {
  if (!params) params = {};
  params.reduce = false;
  params.include_docs = true;

  return this.view(design, view, params)
  .then(function(rows) {
    return rows.map(function(row) {
      return row.doc;
    });
  });
};

Qouch.prototype.request = function(method, path, body) {
  var opts = {
    method: method,
    url: path ? util.format('%s/%s', this.url, path) : this.url,
    headers: {
      'content-type': 'application/json',
      'accepts': 'application/json'
    }
  };
  if (body) opts.body = [ JSON.stringify(body) ];

  return http.request(opts)
  .then(function(res) {
    return Q.post(res.body, 'read', [])
    .then(function(buffer) {
      if ( isNaN(res.status) || res.status >= 400 ) {
        var msg = util.format('HTTP request failed with code %s  %s', res.status, buffer && buffer.toString().trim() )
        throw new QouchRequestError(msg, dbURL, res && res.status, opts, res);
      }
      return JSON.parse(buffer.toString());
    });
  });
};

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
