/**
 * Copyright [2013] [runrightfast.co]
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

'use strict';
var expect = require('chai').expect;

describe('CouchbaseConnectionManager', function() {
	var couchbaseConnectionManager = require('..').couchbaseConnectionManager;

	afterEach(function(done) {
		couchbaseConnectionManager.stop(function() {
			done();
		});
		couchbaseConnectionManager.clear();
	});

	it('can be used to register a connection', function(done) {
		var options = {
			couchbase : {
				"host" : [ "localhost:8091" ],
				buckets : [ {
					"bucket" : "default",
					aliases : [ 'default', 'test' ]
				} ]
			},
			connectionListener : function() {
				console.log('CONNECTED TO COUCHBASE');
			},
			connectionErrorListener : function(error) {
				console.error(error);
				done(error);
			},
			logLevel : 'DEBUG'
		};

		couchbaseConnectionManager.registerConnection(options);
		couchbaseConnectionManager.start(function(cbConn) {
			expect(cbConn).to.exist;
			expect(couchbaseConnectionManager.getBucketConnection('default')).to.exist;
			expect(couchbaseConnectionManager.getBucketConnection('test')).to.exist;
			expect(couchbaseConnectionManager.getBucketConnection('test')).to.equal(couchbaseConnectionManager.getBucketConnection('default'));
			expect(couchbaseConnectionManager.getConnection([ "localhost:8091" ], 'default')).to.exist;
			console.log('*** cbConn ***');
			console.log(cbConn);
			console.log("*** couchbaseConnectionManager.getBucketConnection('default') ***");
			console.log(couchbaseConnectionManager.getBucketConnection('default'));

			var cb = couchbaseConnectionManager.getBucketConnection('default');
			cb.set('CouchbaseConnectionManager', {
				msg : 'can be used to register a connection'
			}, options, function(error, result) {
				if (error) {
					done(error);
				} else {
					done();
				}
			});
		});

	});

	it('can be used to register a connection with no aliases', function(done) {
		var options = {
			couchbase : {
				"host" : [ "localhost:8091" ],
				buckets : [ {
					"bucket" : "default"
				} ]
			},
			connectionListener : function() {
				console.log('CONNECTED TO COUCHBASE');
			},
			connectionErrorListener : function(error) {
				console.error(error);
				done(error);
			},
			logLevel : 'DEBUG'
		};

		couchbaseConnectionManager.registerConnection(options);
		couchbaseConnectionManager.start(function(cbConn) {
			expect(cbConn).to.exist;
			expect(couchbaseConnectionManager.getBucketConnection('default')).to.exist;
			expect(couchbaseConnectionManager.getConnection([ "localhost:8091" ], 'default')).to.exist;
			console.log('*** cbConn ***');
			console.log(cbConn);
			console.log("*** couchbaseConnectionManager.getBucketConnection('default') ***");
			console.log(couchbaseConnectionManager.getBucketConnection('default'));

			var cb = couchbaseConnectionManager.getBucketConnection('default');
			cb.set('CouchbaseConnectionManager', {
				msg : 'can be used to register a connection'
			}, options, function(error, result) {
				if (error) {
					done(error);
				} else {
					done();
				}
			});
		});

	});
});