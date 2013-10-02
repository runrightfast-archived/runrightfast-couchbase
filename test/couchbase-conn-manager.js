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
		var doneInvoked = 0;
		var connCount = couchbaseConnectionManager.getConnectionCount();
		couchbaseConnectionManager.stop(function() {
			doneInvoked++;
			console.log("******* doneInvoked = " + doneInvoked);
			if (connCount === doneInvoked) {
				done();
			}
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
		expect(couchbaseConnectionManager.getBucketCount()).to.equal(2);
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

	it('can be used to register a connection and invokes the startCallback after the connection is started', function(done) {
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
			logLevel : 'DEBUG',
			startCallback : function(cbConn) {
				expect(couchbaseConnectionManager.getBucketCount()).to.equal(2);
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
				}, null, function(error, result) {
					if (error) {
						done(error);
					} else {
						done();
					}
				});
			}
		};

		couchbaseConnectionManager.registerConnection(options);

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

	it('can be used to register multiple connections', function(done) {
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
			logLevel : 'ERROR',
			start : true,
			startCallback : function(result) {
				console.log(result);
			}
		};
		couchbaseConnectionManager.registerConnection(options);

		couchbaseConnectionManager.registerConnection({
			couchbase : {
				"host" : [ "localhost:8091" ],
				buckets : [ {
					"bucket" : "test"
				} ]
			},
			connectionListener : function() {
				console.log('CONNECTED TO COUCHBASE');
			},
			connectionErrorListener : function(error) {
				console.error(error);
				done(error);
			},
			logLevel : 'ERROR'
		});

		expect(couchbaseConnectionManager.getBucketCount()).to.equal(2);

		var connCount = 0;
		var doneCalled = false;
		couchbaseConnectionManager.start(function(cbConn) {
			expect(cbConn).to.exist;
			expect(couchbaseConnectionManager.getBucketConnection(cbConn.options.couchbase.bucket)).to.exist;
			expect(couchbaseConnectionManager.getConnection(cbConn.options.couchbase.host, cbConn.options.couchbase.bucket)).to.exist;

			console.log('*** cbConn ***');
			console.log(cbConn);
			console.log("*** couchbaseConnectionManager.getBucketConnection('" + cbConn.options.couchbase.bucket + "') ***");
			console.log(couchbaseConnectionManager.getBucketConnection(cbConn.options.couchbase.bucket));

			var cb = couchbaseConnectionManager.getBucketConnection(cbConn.options.couchbase.bucket);
			cb.set('CouchbaseConnectionManager', {
				msg : 'can be used to register a connection'
			}, options, function(error, result) {
				if (error) {
					done(error);
				} else {
					connCount++;
					if (connCount === 2) {
						console.log('*********** done called connCount = ' + connCount);
						if (!doneCalled) {
							doneCalled = true;
							done();
						}
					} else {
						console.log('*********** done not called - connCount = ' + connCount);
					}
				}
			});
		});

	});
});