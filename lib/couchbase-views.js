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

/**
 * Each view will be in its own design doc named after the view name.
 * 
 * These views assume each bucket will only contain one type of Entity. For
 * heterogeneous buckets, filter on Entity.type
 */
module.exports = [ {
	name : 'createdOn',
	map : function(doc, meta) {
		if (meta.type !== 'json') {
			return;
		}

		if (doc.createdOn) {
			emit(dateToArray(new Date(doc.createdOn)), 1);
		}
	},
	reduce : '_count'
}, {
	name : 'updatedOn',
	map : function(doc, meta) {
		if (meta.type !== 'json') {
			return;
		}

		if (doc.updatedOn) {
			emit(dateToArray(new Date(doc.updatedOn)), 1);
		}
	},
	reduce : '_count'
} ];