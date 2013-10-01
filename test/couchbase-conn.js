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

describe('CouchbaseConnection', function() {
	var cbConn = null;

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

	it('can be used to create a connection', function(done) {
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

		cbConn = require('..').couchbaseConnection(options);
		expect(cbConn).to.exist;
		cbConn.start(function() {
			cbConn.cb.set('CouchbaseConnection', "can be used to create a connection", options, function(error, result) {
				if (error) {
					done(error);
				} else {
					done();
				}
			});
		});
	});
});