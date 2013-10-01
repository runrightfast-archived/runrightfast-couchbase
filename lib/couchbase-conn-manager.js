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

/**
 * <code>
 * options = {
 *   host : [ 'localhost:8091' ],						// REQUIRED
 *   buckets : [										// REQUIRED
 *   	 {  bucket : 'default',							// REQUIRED - physical bucket name, which must be unique
 *			password : 'password',						// OPTIONAL
 *			aliases : ['alias'] }						// OPTIONAL - alias bucket names, which must be unique. Default is the bucket name	 
 *   ], 
 *   connectionListener : function(error,bucket){},		// OPTIONAL
 *   connectionErrorListener : function(bucket){},		// OPTIONAL
 *   logLevel : 'WARN' 									// OPTIONAL - Default is 'WARN'
 * }
 * </code>
 */
(function() {
	'use strict';

	var lodash = require('lodash');
	var Hoek = require('hoek');
	var assert = Hoek.assert;
	var CouchbaseConnection = require('./couchbase-conn');

	var ConnectionManager = function() {
		this._conns = [];
		this._buckets = {};
	};

	/**
	 * 
	 * @param host
	 * @param bucket
	 */
	ConnectionManager.prototype.getConnection = function getConnection(host, bucket) {
		return lodash.find(this._conns, function(conn) {
			var cbOptions = conn.options.couchbase;
			return cbOptions.bucket === bucket && cbOptions.host.length === host.length && lodash.difference(cbOptions.host, host).length === 0;
		});
	};

	/**
	 * 
	 * @param bucket
	 * @returns Couchbase.Connection
	 */
	ConnectionManager.prototype.getBucketConnection = function(bucket) {
		return this._buckets[bucket].cb;
	};

	/**
	 * <code>
	 * options = { 
	 * 	 couchbase : {										// REQUIRED
	 * 		host : [ 'localhost:8091' ],					// REQUIRED 
	 *		 buckets : [									// REQUIRED
	 *   	 	{  	bucket : 'default',						// REQUIRED - physical bucket name, which must be unique for the host
	 *				password : 'password',					// OPTIONAL
	 *				aliases : ['alias'] }					// OPTIONAL - alias bucket names, which must be unique for all hosts. Default is the bucket name	 
	 *   	], 
	 *   },
	 *   connectionListener : function(error){},			// OPTIONAL
	 *   connectionErrorListener : function(){},			// OPTIONAL
	 *   logLevel : 'WARN'									// OPTIONAL - Default is WARN
	 * }
	 * 
	 * </code>
	 */
	ConnectionManager.prototype.registerConnection = function(options) {
		var i;

		var self = this;
		/**
		 * @param bucket
		 *            options.couchbase.buckets[i]
		 */
		var getAliases = function(bucket) {
			if (lodash.isUndefined(bucket.aliases) || bucket.aliases.length === 0) {
				return [ bucket.bucket ];
			}

			return bucket.aliases;
		};

		var validateOptions = function() {
			/**
			 * @param bucketAliases
			 *            Array of Strings
			 */
			var checkBucketAliasesAreUnique = function(bucketAliases) {
				for (i = 0; i < bucketAliases.length; i++) {
					assert(lodash.isUndefined(self._buckets[bucketAliases[i]]), 'bucket is already registered : ' + bucketAliases[i]);
				}
			};

			assert(lodash.isObject(options), 'options is required');
			assert(lodash.isObject(options.couchbase), 'options.couchbase is required');
			assert(lodash.isObject(options.couchbase), 'options.couchbase is required');
			assert(lodash.isObject(options.couchbase.host), 'options.couchbase.host is required');
			assert(lodash.isArray(options.couchbase.buckets), 'options.couchbase.buckets is required and must be an Array');

			if (!lodash.isUndefined(options.couchbase.password)) {
				assert(lodash.isString(options.couchbase.password), 'options.couchbase.password is required and must be a String');
			}

			for (i = 0; i < options.couchbase.buckets.length; i++) {
				if (!lodash.isUndefined(options.couchbase.buckets[i].aliases)) {
					assert(lodash.isArray(options.couchbase.buckets[i].aliases), 'options.couchbase.buckets[' + i + '] is required and must be an Array');
				}
			}

			for (i = 0; i < options.couchbase.buckets.length; i++) {
				checkBucketAliasesAreUnique(getAliases(options.couchbase.buckets[i]));
			}
		};

		var conn, host, bucketAliases;

		validateOptions();
		host = options.couchbase.host;
		for (i = 0; i < options.couchbase.buckets.length; i++) {
			conn = this.getConnection(host, options.couchbase.buckets[i].bucket);
			if (lodash.isUndefined(conn)) {
				conn = new CouchbaseConnection({
					couchbase : {
						host : host,
						bucket : options.couchbase.buckets[i].bucket,
						password : options.couchbase.buckets[i].password
					},
					connectionListener : options.connectionListener,
					connectionErrorListener : options.connectionErrorListener,
					logLevel : options.logLevel
				});
				this._conns.push(conn);
			}

			bucketAliases = getAliases(options.couchbase.buckets[i]);
			for (i = 0; i < bucketAliases.length; i++) {
				this._buckets[bucketAliases[i]] = conn;
			}
		}
	};

	/**
	 * Starts each of the registered connections
	 * 
	 * @param callback -
	 *            OPTIONAL: function(result){} - where result is either an Error
	 *            or CouchbaseLogger
	 */
	ConnectionManager.prototype.start = function(callback) {
		var i;
		for (i = 0; i < this._conns.length; i++) {
			this._conns[i].start(callback);
		}
	};

	/**
	 * Stop each of the registered connections
	 * 
	 * @param callback
	 *            OPTIONAL
	 */
	ConnectionManager.prototype.stop = function(callback) {
		var i;
		for (i = 0; i < this._conns.length; i++) {
			this._conns[i].stop(callback);
		}
	};

	ConnectionManager.prototype.clear = function() {
		this._conns = [];
		this._buckets = {};
	};

	var connManager = new ConnectionManager();

	module.exports = connManager;
}());
