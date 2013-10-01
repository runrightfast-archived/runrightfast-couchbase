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
 * 	 couchbase : {										// REQUIRED
 * 		host : [ 'localhost:8091' ],					// REQUIRED 
 *		bucket : 'default',								// REQUIRED 
 *		password : 'password' 							// OPTIONAL
 *   },
 *   connectionListener : function(error){},			// OPTIONAL
 *   connectionErrorListener : function(){},			// OPTIONAL
 *   logLevel : 'WARN' 									// OPTIONAL - Default is WARN
 *   
 * }
 * </code>
 */
(function() {
	'use strict';

	var logging = require('runrightfast-commons').logging;
	var log = logging.getLogger('couchbase-conn');
	var events = require('runrightfast-commons').events;
	var lodash = require('lodash');
	var util = require('util');
	var Hoek = require('hoek');
	var assert = Hoek.assert;
	var Couchbase = require('couchbase');

	var EVENTS = {
		CONN_ERR : 'CONN_ERR',
		STARTING : 'STARTING',
		STARTED : 'STARTED',
		STOPPED : 'STOPPED'
	};

	var CouchbaseConnection = function(options) {
		events.AsyncEventEmitter.call(this);

		var validateOptions = function() {
			assert(lodash.isObject(options), 'options is required');
			assert(lodash.isObject(options.couchbase), 'options.couchbase is required');
			assert(lodash.isArray(options.couchbase.host), 'options.couchbase.host is required and must be an Array');
			assert(lodash.isString(options.couchbase.bucket), 'options.couchbase.bucket is required and must be a String');
			if (!lodash.isUndefined(options.couchbase.password)) {
				assert(lodash.isString(options.couchbase.password), 'options.couchbase.password is required and must be a String');
			}
			if (options.connectionListener) {
				assert(lodash.isFunction(options.connectionListener), 'options.connectionListener must be a function');
			}
			if (options.connectionErrorListener) {
				assert(lodash.isFunction(options.connectionErrorListener), 'options.connectionErrorListener must be a function');
			}
		};

		var logOptions = function() {
			var logLevel = options.logLevel || 'WARN';
			logging.setLogLevel(log, logLevel);
			if (log.isLevelEnabled('DEBUG')) {
				log.debug(options);
			}
		};

		validateOptions();
		logOptions();

		this.options = options;

		if (lodash.isFunction(options.connectionListener)) {
			this.on(EVENTS.STARTED, options.connectionListener);
		}

		if (lodash.isFunction(options.connectionErrorListener)) {
			this.on(EVENTS.CONN_ERR, options.connectionErrorListener);
		}
	};

	util.inherits(CouchbaseConnection, events.AsyncEventEmitter);

	/**
	 * Emits a 'STARTING' event when invoked and the Couchbase connection has
	 * not yet been created.
	 * 
	 * if an error occurs while connecting, then an event is emitted where the
	 * event name is 'CONN_ERR' and the event data : (Error,CouchbaseLogger)
	 * 
	 * if the connection is successful, then an event is emitted where the event
	 * name is 'STARTED', with the CouchbaseListener as the event data
	 * 
	 * @param callback -
	 *            OPTIONAL: function(result){} - where result is either an Error
	 *            or CouchbaseLogger
	 */
	CouchbaseConnection.prototype.start = function(callback) {
		if (callback) {
			assert(lodash.isFunction(callback, 'callback must be a function'));
		}

		var self = this;

		if (!this.cb) {
			this.cb = new Couchbase.Connection(this.options.couchbase, function(error) {
				setImmediate(function() {
					if (error) {
						log.error('Failed to start: ' + error);
						self.emit(EVENTS.CONN_ERR, error, self);
						if (callback) {
							process.nextTick(callback.bind(null, error));
						}
					} else {
						log.info('STARTED');
						self.emit(EVENTS.STARTED, self);
						if (callback) {
							process.nextTick(callback.bind(null, self));
						}
					}
				});
				self.emit(EVENTS.STARTING);
				log.info('STARTING');
			});
		} else {
			if (callback) {
				process.nextTick(callback.bind(null, self));
			}
		}
	};

	/**
	 * emits 'STOPPED' event with the CouchbaseLogger as the event data.
	 * 
	 */
	CouchbaseConnection.prototype.stop = function(callback) {
		if (this.cb) {
			if (callback) {
				assert(lodash.isFunction(callback, 'callback must be a function'));
			}

			this.cb.shutdown();
			this.emit(EVENTS.STOPPED, this);
			this.cb = undefined;
			log.info('STOPPED');
			if (callback) {
				process.nextTick(callback);
			}
		} else {
			if (callback) {
				process.nextTick(callback);
			}
		}
	};

	module.exports = CouchbaseConnection;
}());
