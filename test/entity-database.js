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
var cbConnManager = require('..').couchbaseConnectionManager;
var EntityDatabase = require('..').EntityDatabase;
var Entity = require('runrightfast-commons').Entity;
var when = require('when');
var uuid = require('uuid');
var lodash = require('lodash');

describe('database', function() {
	var database = null;

	var idsToDelete = [];

	before(function(done) {

		var options = {
			couchbase : {
				"host" : [ "localhost:8091" ],
				buckets : [ {
					"bucket" : "default"
				} ]
			},
			logLevel : 'DEBUG',
			startCallback : function(result) {
				console.log('before::startCallback');
				console.log(result);

				database = new EntityDatabase({
					couchbaseConn : cbConnManager.getBucketConnection('default'),
					logLevel : 'DEBUG',
					entityConstructor : Entity
				});

				done();
			}
		};

		cbConnManager.registerConnection(options);

	});

	afterEach(function(done) {
		database.deleteEntities(idsToDelete).then(function(result) {
			idsToDelete = [];
			done();
		}, function(error) {
			console.error(JSON.stringify(error, undefined, 2));
			done(error.error);
		});

	});

	after(function(done) {
		database.deleteEntities(idsToDelete).then(function(result) {
			cbConnManager.stop(function() {
				cbConnManager.clear();
				idsToDelete = [];
				done();
			});
		}, function(error) {
			done(error);
		});

	});

	it.only('can create a new Entity in the database', function(done) {
		var entity = new Entity();
		idsToDelete.push(entity.id);
		database.createEntity(entity).then(function(result) {
			console.log(JSON.stringify(result, undefined, 2));
			try {
				expect(result.cas).to.exist;
				expect(result.value instanceof Entity).to.equal(true);
				done();
			} catch (err) {
				done(err);
			}

		}, function(err) {
			done(error);
		});
	});

});