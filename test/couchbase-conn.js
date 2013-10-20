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
var Couchbase = require('couchbase');

describe('CouchbaseConnection', function() {
	var CouchbaseConnection = require('..').CouchbaseConnection;
	var cbConn = null;

	before(function(done) {
		var options = {
			couchbase : {
				"host" : [ "localhost:8091" ],
				"bucket" : "default"
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

		cbConn = new CouchbaseConnection(options);
		expect(cbConn).to.exist;
		cbConn.start(function() {
			done();
		});
	});

	after(function(done) {
		if (cbConn) {
			cbConn.on('STOPPED', function() {
				console.log('Couchbase connection has been shutdown.');
				done();
			});

			cbConn.stop();
		} else {
			done();
		}
	});

	it('can be used to set a doc', function(done) {
		cbConn.cb.set('CouchbaseConnection', {
			msg : 'can be used to create a connection'
		}, undefined, function(error, result) {
			if (error) {
				done(error);
			} else {
				cbConn.cb.remove('CouchbaseConnection', undefined, function(error, result) {
					console.log(JSON.stringify({
						error : error,
						result : result
					}, undefined, 2));

					if (error) {
						done(error);
					} else {
						cbConn.cb.remove('CouchbaseConnection', undefined, function(error, result) {
							console.log(JSON.stringify({
								error : error,
								result : result
							}, undefined, 2));

							if (error) {
								if (error.code === Couchbase.errors.keyNotFound) {
									done();
								} else {
									console.log('remove failed: ' + error);
									done(error);
								}
							} else {
								done(new Error('remove should have resulted in an error because the doc was already deleted'));
							}
						});
					}
				});

			}
		});
	});

	it('removing a doc using a doc id that does not exist', function(done) {
		cbConn.cb.remove('ADASDASDASDASDASD', undefined, function(error, result) {
			console.log(JSON.stringify({
				error : error,
				result : result
			}, undefined, 2));

			if (error) {
				if (error.code === Couchbase.errors.keyNotFound) {
					done();
				} else {
					console.log('remove failed: ' + error);
					done(error);
				}

			} else {
				done(new Error('remove should have resulted in an error'));
			}
		});

	});
});