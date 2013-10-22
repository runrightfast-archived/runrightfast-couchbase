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
 * 	 couchbaseConn: conn,								// REQUIRED - Couchbase.Connection
 *   logLevel : 'WARN',									// OPTIONAL - Default is 'WARN',
 *   entityConstructor									// REQUIRED - Entity constructor
 * }
 * </code>
 */
(function() {
	'use strict';

	var logging = require('runrightfast-commons').logging;
	var log = logging.getLogger('entity-database');
	var lodash = require('lodash');
	var Hoek = require('hoek');
	var assert = Hoek.assert;
	var Couchbase = require('couchbase');
	var when = require('when');
	var joi = require('joi');

	var dateType = joi.types.Object();
	dateType.isDate = function() {
		this.add('isDate', function(value, obj, key, errors, keyPath) {
			if (lodash.isDate(value)) {
				return true;
			}
			errors.add(key + ' must be a Date', keyPath);
			return false;

		}, arguments);
		return this;
	};

	var DateType = dateType.isDate();

	var queryOptionsSchema = {
		from : DateType,
		to : DateType,
		inclusiveEnd : joi.types.Boolean(),
		startDocId : joi.types.String(),
		skip : joi.types.Number().min(0),
		limit : joi.types.Number().min(0),
		descending : joi.types.Boolean(),
		dateField : joi.types.String().required(),
		returnDocs : joi.types.Boolean()
	};

	var validateIdsArray = function validateIdsArray(ids) {
		var schema = {
			ids : joi.types.Array().required().includes(joi.types.String())
		};
		var validationError = joi.validate({
			ids : ids
		}, schema);
		if (validationError) {
			throw validationError;
		}
	};

	var dateToArray = function(date) {
		return [ date.getFullYear(), date.getMonth() + 1, date.getDate(), date.getHours(), date.getMinutes(), date.getSeconds() ];
	};

	var EntityDatabase = function(options) {
		var validateOptions = function() {
			assert(lodash.isObject(options), 'options is required');
			assert(lodash.isObject(options.couchbaseConn), 'options.couchbaseConn is required');
			assert(lodash.isFunction(options.entityConstructor), 'options.entityConstructor is required and must be an Entity constructor');
		};

		var logOptions = function() {
			var logLevel = options.logLevel || 'WARN';
			logging.setLogLevel(log, logLevel);
			if (log.isDebugEnabled()) {
				log.debug(options);
			}
		};

		validateOptions();
		logOptions();
		this.Entity = options.entityConstructor;
		this.cb = options.couchbaseConn;
	};

	/**
	 * 
	 * 
	 * @param entity
	 * @return Promise that returns the created Entity and cas - as an object
	 *         with 'schema' and 'cas' properties. If an Error occurs, it will
	 *         have an error code - defined by errorCodes keys
	 */
	EntityDatabase.prototype.createEntity = function(entity) {
		var self = this;
		return when.promise(function(resolve, reject) {
			if (!lodash.isObject(entity)) {
				reject(new Error('entity is required'));
				if (log.isDebugEnabled()) {
					log.debug('createEntity(): entity is not an object\n' + JSON.stringify(entity, undefined, 2));
				}
				return;
			}

			if (log.isDebugEnabled()) {
				log.debug('createEntity():\n' + JSON.stringify(entity, undefined, 2));
			}

			var newSchema;
			try {
				newSchema = new self.Entity(entity);
			} catch (err) {
				err.code = 'INVALID_OBJ_SCHEMA';
				reject(err);
				return;
			}

			self.cb.add(entity.id, entity, undefined, function(error, result) {
				if (error) {
					log.error('createEntity() : ' + error);
					if (error.code === Couchbase.errors.keyAlreadyExists) {
						error.code = 'DUP_NS_VER';
					}
					reject(error);
				} else {
					if (log.isDebugEnabled) {
						log.debug('createEntity() : ' + JSON.stringify(result));
					}
					resolve({
						value : newSchema,
						cas : result.cas
					});
				}
			});
		});
	};

	/**
	 * 
	 * @param namespace
	 *            REQUIRED
	 * @param version
	 *            REQUIRED
	 * @return Promise that returns the Entity and cas if found - as an object
	 *         with 'value' and 'cas' properties. If the Entity does not exist,
	 *         then an Error with code NOT_FOUND is returned
	 * 
	 * returned object has the following properties: <code>
	 * cas			Couchbase CAS
	 * value		Entity object
	 * </code>
	 */
	EntityDatabase.prototype.getEntity = function(id) {
		var self = this;
		return when.promise(function(resolve, reject) {
			if (!lodash.isString(id)) {
				reject(new Error('id is required and must be a String'));
				return;
			}
			self.cb.get(id, function(error, result) {
				if (error) {
					if (error.code === Couchbase.errors.keyNotFound) {
						error.code = 'NOT_FOUND';
					} else {
						log.error('getEntity() : ' + error);
					}

					reject(error);
				} else {
					if (log.isDebugEnabled) {
						log.debug('getEntity() : ' + JSON.stringify(result, undefined, 2));
					}
					resolve({
						value : new self.Entity(result.value),
						cas : result.cas
					});
				}
			});

		});
	};

	/**
	 * The updatedOn will be set to the current time
	 * 
	 * @param entity
	 * @param cas
	 *            REQUIRED used to ensure that no one else has updated the
	 *            schema since it was retrieved
	 * @param updatedBy
	 *            OPTIONAL
	 * @return Promise - If successful, the returned object has the following
	 *         properties:
	 * 
	 * <code>
	 * cas			Couchbase CAS
	 * value		Entity object
	 * </code>
	 */
	EntityDatabase.prototype.updateEntity = function(entity, cas, updatedBy) {
		var self = this;
		return when.promise(function(resolve, reject) {
			try {
				assert(lodash.isObject(entity), 'entity is required');
				assert(lodash.isObject(cas), 'cas is required');
				if (!lodash.isUndefined(updatedBy)) {
					assert(lodash.isString(updatedBy), 'updatedBy must be a String');
				}
			} catch (err) {
				reject(err);
				return;
			}

			var newSchema;
			try {
				newSchema = new self.Entity(entity);
			} catch (err) {
				err.code = 'INVALID_OBJ_SCHEMA';
				reject(err);
				return;
			}

			newSchema.updated(updatedBy);

			var replaceOptions = {
				cas : cas
			};

			self.cb.replace(entity.id, entity, replaceOptions, function(error, result) {
				if (error) {
					if (error.code === Couchbase.errors.keyAlreadyExists) {
						error.code = 'STALE_OBJ';
						if (log.isInfoEnabled()) {
							log.info('updateEntity() : ' + error);
						}
					} else {
						log.error('updateEntity() : ' + error);
					}
					reject(error);
				} else {
					if (log.isDebugEnabled) {
						log.debug('createEntity() : ' + JSON.stringify(result, undefined, 2));
					}
					resolve({
						value : newSchema,
						cas : result.cas
					});
				}
			});
		});
	};

	/**
	 * 
	 * @param ids
	 *            REQUIRED - Array of entity ids.
	 * @return Promise that returns an dictionary of entities that were found.:
	 * 
	 * <code>
	 * entityId -> {
	 * 						cas 			// Couchbase CAS
	 * 						value			// Entity
	 *  				 }
	 * <code>
	 *
	 */
	EntityDatabase.prototype.getEntitiess = function(ids) {
		var self = this;
		return when.promise(function(resolve, reject) {
			try {
				validateIdsArray(ids);
			} catch (error) {
				reject(error);
				return;
			}

			var convert = function convert(result) {
				var value;
				return lodash.foldl(lodash.keys(result), function(response, key) {
					value = result[key];
					if (value.error) {
						return response;
					}

					response[key] = {
						cas : value.cas,
						value : new self.Entity(value.value)
					};
					return response;
				}, {});
			};

			self.cb.getMulti(ids, null, function(error, result) {
				if (log.isDebugEnabled()) {
					log.debug('getEntities(): getMulti():\n' + JSON.stringify({
						error : error,
						result : result
					}, undefined, 2));
				}
				if (error) {
					var errors = lodash.foldl(lodash.keys(result), function(errorsNot_KeyNotFound, id) {
						if (result[id].error && result[id].error.code !== Couchbase.errors.keyNotFound) {
							errorsNot_KeyNotFound.push(result[id]);
						}
						return errorsNot_KeyNotFound;
					}, []);

					if (errors.length > 0) {
						log.error('getEntities() : ' + error);
						reject({
							error : error,
							result : convert(result)
						});
					} else {
						resolve(convert(result));
					}
				} else {
					resolve(convert(result));
				}
			});
		});

	};

	/**
	 * 
	 * 
	 * @param namespace
	 * @param version
	 * @return Promise
	 */
	EntityDatabase.prototype.deleteEntity = function(id) {
		var self = this;
		return when.promise(function(resolve, reject) {
			if (!lodash.isString(id)) {
				reject(new Error('id is required and must be a String'));
				return;
			}
			self.cb.remove(id, undefined, function(error, result) {
				if (error) {
					if (error.code === Couchbase.errors.keyNotFound) {
						resolve();
					} else {
						log.error('deleteEntity() : ' + error);
						reject(error);
					}
				} else {
					if (log.isLevelEnabled('DEBUG')) {
						log.debug('deleteEntity() : ' + JSON.stringify(result));
					}
					resolve(result);
				}
			});
		});

	};

	/**
	 * 
	 * 
	 * @param ids
	 *            REQUIRED - REQUIRED - Array of entity ids.
	 * 
	 * @return Promise where the result is the Couchbase cas for each deleted
	 *         document. If an error occurs, then an object containing both the
	 *         error and result is returned in order to inspect what went wrong.
	 */
	EntityDatabase.prototype.deleteEntities = function(ids) {
		var self = this;
		return when.promise(function(resolve, reject) {
			try {
				validateIdsArray(ids);
			} catch (err) {
				reject(err);
				return;
			}

			if (log.isDebugEnabled()) {
				log.debug('deleteEntities() : ids = ' + JSON.stringify(ids, undefined, 2));
			}
			if (lodash.keys(ids).length === 0) {
				resolve();
				return;
			}
			self.cb.removeMulti(ids, undefined, function(error, result) {
				if (error) {
					var errors = lodash.foldl(lodash.keys(result), function(errorsNot_KeyNotFound, id) {
						if (result[id].error && result[id].error.code !== Couchbase.errors.keyNotFound) {
							errorsNot_KeyNotFound.push(result[id]);
						}
						return errorsNot_KeyNotFound;
					}, []);

					if (errors.length > 0) {
						log.error('deleteEntity() : ' + error);
						reject({
							error : error,
							result : result
						});
					} else {
						resolve(result);
					}
				} else {
					if (log.isDebugEnabled) {
						log.debug('deleteEntities() : ' + JSON.stringify(result, undefined, 2));
					}
					resolve(result);
				}
			});
		});

	};

	/**
	 * NOTE: this api can also be used to page through all Entities in the
	 * database by not specifying any date range, i.e., from and to
	 * 
	 * @param params
	 *            where params is an objet with the following properties:
	 * 
	 * <code> 
	 * from				OPTIONAL - Date - the start date to search from
	 * to           	OPTIONAL - Date - defaults to now
	 * inclusiveEnd		OPTIONAL - Boolean - default is true
	 * startDocId		OPTIONAL - String - the document id to start with 
	 * skip				OPTIONAL - Integer - Skip this number of records before starting to return the results
	 * limit	     	OPTIONAL - Integer - Limit the number of the returned documents to the specified number - defaults to 20
	 * descending		OPTIONAL - Boolean - default is false,
	 * dateField		OPTIONAL - String - one of ['createdOn','updatedOn']
	 * returnDocs		OPTIONAL - Boolean - if true, then the documents are returned - default is false
	 * </code>
	 * 
	 * @param {String}dateField
	 *            REQUIRED -The Coucbase DesignDoc and View will be named after
	 *            the field.
	 * 
	 * @return Promise if rerturnDocs=true, then an array of the following is
	 *         returned:
	 * 
	 * <code> 
	 * {
	 *   cas					// Couchbase CAS									
	 *   value					// Entity object
	 * }
	 * </code>
	 * 
	 * else an array of objects with the following properties is returned:
	 * 
	 * <code> 
	 * {
	 *   id	: 'ns://runrightfast.co/schema1/1.0.4',				// Entity id									
	 *   key: date												// Date - Index key
	 * }
	 * </code>
	 */
	EntityDatabase.prototype.queryByDateField = function(params) {
		var viewOptions = {
			stale : false,
			reduce : false,
			limit : 20,
			inclusive_end : true
		};

		var self = this;
		return when.promise(function(resolve, reject) {
			if (params) {
				var error = joi.validate(params, queryOptionsSchema);
				if (error) {
					return reject(error);
				}
			}

			if (params) {
				if (params.from) {
					viewOptions.startkey = dateToArray(params.from);
				}
				if (params.to) {
					viewOptions.endkey = dateToArray(params.to);
				}
				if (params.inclusiveEnd) {
					viewOptions.inclusive_end = params.inclusiveEnd;
				}
				if (params.startDocId) {
					viewOptions.startkey_docid = params.startDocId;
				}
				if (params.skip) {
					viewOptions.skip = params.skip;
				}
				if (params.limit) {
					viewOptions.limit = params.limit;
				}
				if (params.descending) {
					viewOptions.descending = params.descending;
				}
			}

			if (log.isDebugEnabled()) {
				log.debug('getEntitiesByCreatedOn(): viewOptions:\n' + JSON.stringify(viewOptions, undefined, 2));
			}

			var q = self.cb.view(params.dateField, params.dateField, viewOptions);
			q.query(function(err, results) {
				if (err) {
					reject(err);
				} else {
					if (params.returnDocs) {
						when(self.getEntities(lodash.map(results, function(result) {
							return result.id;
						})), function(entities) {
							resolve(lodash.map(results, function(result) {
								return entities[result.id];
							}));
						}, function(error) {
							reject(error);
						});
					} else {
						resolve(lodash.map(results, function(result) {
							return {
								id : result.id,
								key : new Date(result.key[0], result.key[1], result.key[2], result.key[3], result.key[4], result.key[5], 0)
							};
						}));
					}
				}
			});
		});
	};

	/**
	 * delegates to queryByDateField() with params.dateField = 'createdOn'
	 * 
	 * @see queryByDateField
	 */
	EntityDatabase.prototype.getEntitiesByCreatedOn = function(params) {
		if (params) {
			params.dateField = 'createdOn';
			return this.queryByDateField(params);
		}

		return this.queryByDateField({
			dateField : 'createdOn'
		});
	};

	/**
	 * delegates to queryByDateField() with params.dateField = 'updatedOn'
	 * 
	 * @see queryByDateField
	 */
	EntityDatabase.prototype.getEntitiesByUpdatedOn = function(params) {
		if (params) {
			params.dateField = 'updatedOn';
			return this.queryByDateField(params);
		}

		return this.queryByDateField({
			dateField : 'updatedOn'
		});
	};

	EntityDatabase.prototype.errorCodes = {
		DUP_NS_VER : 'An Entity with the same name id (namespace/version) already exists.',
		INVALID_OBJ_SCHEMA : 'The object schema is invalid.',
		UNEXPECTED_ERR : 'Unexpected error.',
		NOT_FOUND : 'Not found',
		STALE_OBJ : 'Entity is stale - an newer version is available'
	};

	/**
	 * Checks if each of the design docs exist.
	 * 
	 * @returns Promise where the result is an object consisting of 2
	 *          properties:
	 * 
	 * <code>
	 * designDocs 		Object - map of design doc name -> design doc
	 * errors			Object - map of design doc name -> Error
	 * </code>
	 */
	EntityDatabase.prototype.checkDesignDocs = function(create, replace, designDocs) {
		var self = this;
		var results = {};
		var errors = {};
		var getDesignDocsPromises = [];
		var deleteDesignDocsPromises = [];
		var setDesignDocsPromises = [];
		var createDesignDocsPromises = [];
		return when.promise(function(resolve, reject) {
			if (!lodash.isUndefined(create)) {
				if (!lodash.isBoolean(create)) {
					reject(new Error('create arg must be a Boolean'));
					return;
				}
			}

			if (!lodash.isUndefined(replace)) {
				if (!lodash.isBoolean(replace)) {
					reject(new Error('replace arg must be a Boolean'));
					return;
				}
			}

			var couchbaseViews = designDocs || require('./couchbase-views');

			if (replace) {
				lodash.forEach(couchbaseViews, function(view) {
					deleteDesignDocsPromises.push(when.promise(function(resolve) {
						self.cb.removeDesignDoc(view.name, function() {
							resolve();
						});
					}));
				});
			}

			when.all(deleteDesignDocsPromises).then(function() {
				lodash.forEach(couchbaseViews, function(view) {
					if (log.isDebugEnabled()) {
						log.debug('checkDesignDocs() : view :\n' + JSON.stringify(view, undefined, 2));
					}

					getDesignDocsPromises.push(when.promise(function(resolve) {
						self.cb.getDesignDoc(view.name, function(err, data) {
							if (err) {
								if (create) {
									var designDoc = {
										views : {}
									};

									designDoc.views[view.name] = {
										map : view.map.toString()
									};
									if (view.reduce) {
										designDoc.views[view.name].reduce = view.reduce.toString();
									}

									if (log.isDebugEnabled()) {
										log.debug('creating design doc:\n' + JSON.stringify(designDoc, undefined, 2));
									}

									setDesignDocsPromises.push(when.promise(function(resolve) {
										self.cb.setDesignDoc(view.name, designDoc, function(err) {
											if (err) {
												errors[view.name] = err;
											} else {
												createDesignDocsPromises.push(when.promise(function(resolve) {
													self.cb.getDesignDoc(view.name, function(err, data) {
														if (err) {
															errors[view.name] = err;
														} else {
															results[view.name] = data;
														}
														resolve();
													});
												}));
											}
											resolve();
										});
									}));

								} else {
									errors[view.name] = err;
								}
							} else {
								results[view.name] = data;
							}
							resolve();
						});
					}));

				});
			});

			when.all(getDesignDocsPromises).then(function() {
				when.all(setDesignDocsPromises).then(function() {
					when.all(createDesignDocsPromises).then(function() {
						resolve({
							designDocs : results,
							errors : errors
						});
					}, function(error) {
						reject(error);
					});
				}, function(error) {
					reject(error);
				});

			}, function(error) {
				reject(error);
			});

		});

	};

	module.exports = EntityDatabase;
}());
