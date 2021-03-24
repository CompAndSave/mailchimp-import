'use strict';

const MongoDBOps = require('mongodb-ops');

class MongoDB extends MongoDBOps {
  /**
   * @class
   * @classdesc MongoDB Operations
   * 
   * @param {string} collectionName Collection name
   * @param {string} connString Database connection string
   */
  constructor(collectionName, connString) {
    super(connString);
    this.collectionName = collectionName;
    this.connString = connString;
  }

  /**
   * Static method - Get data by ID
   * 
   * @param {string} collectionName Collection name
   * @param {string|number} id _id
   * @param {object} [projection] Projection {@link https://docs.mongodb.com/manual/reference/method/db.collection.find/#find-projection}
   * @param {string} connString Database connection string
   * @returns {promise} Promise with object array
   */
  static async getDataByID(collectionName, id, projection, connString) {
    return Promise.resolve(await MongoDBOps.getData(collectionName, { _id: id }, false, projection, undefined, undefined, undefined, connString));
  }

  /**
   * Instance method - Get data by ID
   * 
   * @param {string|number} id _id
   * @param {object} [projection] Projection {@link https://docs.mongodb.com/manual/reference/method/db.collection.find/#find-projection}
   * @returns {promise} Promise with object array
   */
  async getDataByID(id, projection) { return Promise.resolve(await MongoDB.getDataByID(this.collectionName, id, projection, this.connString)); }

  /**
   * Static method - Get all data
   * 
   * @param {string} collectionName Collection name 
   * @param {object} [projection] Projection {@link https://docs.mongodb.com/manual/reference/method/db.collection.find/#find-projection} 
   * @param {object} [sort] Sort filter {@link https://docs.mongodb.com/manual/reference/method/cursor.sort/#cursor.sort} 
   * @param {object} [pagination] Pagination `E.g., { startIndex: 11, endIndex: 20 }`
   * @param {string} connString Database connection string 
   * @returns {promise} Promise with object array
   */
  static async getAllData(collectionName, projection, sort, pagination, connString) {
    return Promise.resolve(await MongoDBOps.getData(collectionName, {}, false, projection, sort, pagination, false, connString));
  }

  /**
   * Instance method - Get all data
   * 
   * @param {object} [projection] Projection {@link https://docs.mongodb.com/manual/reference/method/db.collection.find/#find-projection} 
   * @param {object} [sort] Sort filter {@link https://docs.mongodb.com/manual/reference/method/cursor.sort/#cursor.sort} 
   * @param {object} [pagination] Pagination `E.g., { startIndex: 11, endIndex: 20 }`
   * @returns {promise} Promise with object array
   */
  async getAllData(projection, sort, pagination) { return Promise.resolve(await MongoDB.getAllData(this.collectionName, projection, sort, pagination, this.connString)); }

  /**
   * Static method - Insert one document to database
   * 
   * @param {string} collectionName Collection name
   * @param {object} doc Data document
   * @param {string} connString Database connection string
   * @returns {promise}
   */
  static async insertOne(collectionName, doc, connString) { return Promise.resolve(await MongoDBOps.writeData("insertOne", collectionName, doc, undefined, connString)); }

  /**
   * Instance method - Insert one document to database
   * 
   * @param {object} doc Data document
   * @returns {promise}
   */
  async insertOne(doc) { return Promise.resolve(await super.writeData("insertOne", this.collectionName, doc, this.connString)); }

  /**
   * Static method - Insert multiple documents to database in ordered way
   * 
   * @param {string} collectionName Collection name 
   * @param {Array} docs Data document array
   * @param {string} connString Database connection string 
   * @returns {promise}
   */
  static async insertBulkOrdered(collectionName, docs, connString) { return Promise.resolve(await MongoDBOps.writeBulkData("insertBulk", collectionName, docs, true, connString)); }

  /**
   * Instance method - Insert multiple documents to database in ordered way
   * 
   * @param {Array} docs Data document array
   * @returns {promise}
   */
  async insertBulkOrdered(docs) { return Promise.resolve(await super.writeBulkData("insertBulk", this.collectionName, docs, true, this.connString)); }

  /**
   * Static method - Insert multiple documents to database in unordered way
   * 
   * @param {string} collectionName Collection name 
   * @param {Array} docs Data document array
   * @param {string} connString Database connection string
   * @returns {promise}
   */
  static async insertBulkUnOrdered(collectionName, docs, connString) { return Promise.resolve(await MongoDBOps.writeBulkData("insertBulk", collectionName, docs, false, connString)); }

  /**
   * Instance method - Insert multiple documents to database in unordered way
   * 
   * @param {Array} docs Data document array
   * @returns {promise}
   */
  async insertBulkUnOrdered(docs) { return Promise.resolve(await super.writeBulkData("insertBulk", this.collectionName, docs, false, this.connString)); }

  /**
   * Static method - Replace one document to database
   * 
   * @param {string} collectionName Collection name 
   * @param {object} doc Data document
   * @param {object} filter Query filter {@link https://docs.mongodb.com/manual/core/document/#document-query-filter}
   * @param {string} connString Database connection string
   * @returns {promise}
   */
  static async replaceOne(collectionName, doc, filter, connString) { return Promise.resolve(await MongoDBOps.writeData("replaceOne", collectionName, doc, filter, connString)); }
  
  /**
   * Instance method - Replace one document to database
   * 
   * @param {object} doc Data document
   * @param {object} filter Query filter {@link https://docs.mongodb.com/manual/core/document/#document-query-filter}
   * @returns {promise}
   */
  async replaceOne(doc, filter) { return Promise.resolve(await super.writeData("replaceOne", this.collectionName, doc, filter, this.connString)); }

  /**
   * Static method - Replace multiple documents to database in ordered way
   * 
   * @param {string} collectionName Collection name
   * @param {Array} docs ReplaceOne object array
   * @param {string} connString Database connection string
   * @returns {promise}
   */
  static async replaceBulkOrdered(collectionName, docs, connString) { return Promise.resolve(await MongoDBOps.writeBulkData("replaceBulk", collectionName, docs, true, connString)); }
  
  /**
   * Instance method - Replace multiple documents to database in ordered way
   * 
   * @param {Array} docs ReplaceOne object array
   * @returns {promise}
   */
  async replaceBulkOrdered(docs) { return Promise.resolve(await super.writeBulkData("replaceBulk", this.collectionName, docs, true, this.connString)); }

  /**
   * Static method - Replace multiple documents to database in unordered way
   * 
   * @param {string} collectionName Collection name
   * @param {Array} docs ReplaceOne object array
   * @param {string} connString Database connection string
   * @returns {promise}
   */
  static async replaceBulkUnOrdered(collectionName, docs, connString) { return Promise.resolve(await MongoDBOps.writeBulkData("replaceBulk", collectionName, docs, false, connString)); }
  
  /**
   * Instance method - Replace multiple documents to database in unordered way
   * 
   * @param {Array} docs ReplaceOne object array
   * @returns {promise}
   */
  async replaceBulkUnOrdered(docs) { return Promise.resolve(await super.writeBulkData("replaceBulk", this.collectionName, docs, false, this.connString)); }

  /**
   * Static method - Update one document to database
   * 
   * @param {string} collectionName Collection name
   * @param {object} doc UpdateOne object
   * @param {object} filter Query filter {@link https://docs.mongodb.com/manual/core/document/#document-query-filter} 
   * @param {string} connString Database connection string 
   * @returns {promise}
   */
  static async updateOne(collectionName, doc, filter, connString) { return Promise.resolve(await MongoDBOps.writeData("updateOne", collectionName, doc, filter, connString)); }
  
  /**
   * Instance method - Update one document to database
   * 
   * @param {object} doc UpdateOne object
   * @param {object} filter Query filter {@link https://docs.mongodb.com/manual/core/document/#document-query-filter} 
   * @returns {promise}
   */
  async updateOne(doc, filter) { return Promise.resolve(await super.writeData("updateOne", this.collectionName, doc, filter, this.connString)); }

  /**
   * Static method - Update many documents to database
   * 
   * @param {string} collectionName Collection name
   * @param {object} doc UpdateOne object
   * @param {object} filter Query filter {@link https://docs.mongodb.com/manual/core/document/#document-query-filter} 
   * @param {string} connString Database connection string
   * @returns {promise}
   */
  static async updateMany(collectionName, doc, filter, connString) { return Promise.resolve(await MongoDBOps.writeData("updateMany", collectionName, doc, filter, connString)); }
  
  /**
   * Instance method - Update many documents to database
   * 
   * @param {object} doc UpdateOne object
   * @param {object} filter Query filter {@link https://docs.mongodb.com/manual/core/document/#document-query-filter} 
   * @returns {promise}
   */
  async updateMany(doc, filter) { return Promise.resolve(await super.writeData("updateMany", this.collectionName, doc, filter, this.connString)); }

  /**
   * Static method - Update multiple documents to database in ordered way
   * 
   * @param {string} collectionName Collection name
   * @param {Array} docs UpdateOne object array 
   * @param {string} connString Database connection string 
   * @returns {promise}
   */
  static async updateBulkOrdered(collectionName, docs, connString) { return Promise.resolve(await MongoDBOps.writeBulkData("updateBulk", collectionName, docs, true, connString)); }
  
  /**
   * Instance method - Update multiple documents to database in ordered way
   * 
   * @param {Array} docs UpdateOne object array 
   * @returns {promise}
   */
  async updateBulkOrdered(docs) { return Promise.resolve(await super.writeBulkData("updateBulk", this.collectionName, docs, true, this.connString)); }
  
  /**
   * Static method - Update multiple documents to database in unordered way
   * 
   * @param {string} collectionName Collection name
   * @param {Array} docs UpdateOne object array
   * @param {string} connString Database connection string
   * @returns {promise}
   */
  static async updateBulkUnOrdered(collectionName, docs, connString) { return Promise.resolve(await MongoDBOps.writeBulkData("updateBulk", collectionName, docs, false, connString)); }
  
  /**
   * Instance method - Update multiple documents to database in unordered way
   * 
   * @param {Array} docs UpdateOne object array
   * @returns {promise}
   */
  async updateBulkUnOrdered(docs) { return Promise.resolve(await super.writeBulkData("updateBulk", this.collectionName, docs, false, this.connString)); }

  /**
   * Static method - BulkWrite operations to database in ordered way
   * 
   * @param {string} collectionName Collection name 
   * @param {Array} docs BulkWrite object array
   * @param {string} connString Database connection string 
   * @returns {promise}
   */
  static async allBulkOrdered(collectionName, docs, connString) { return Promise.resolve(await MongoDBOps.writeBulkData("allBulk", collectionName, docs, true, connString)); }
  
  /**
   * Instance method - BulkWrite operations to database in ordered way
   * 
   * @param {Array} docs BulkWrite object array
   * @returns {promise}
   */
  async allBulkOrdered(docs) { return Promise.resolve(await super.writeBulkData("allBulk", this.collectionName, docs, true, this.connString)); }
  
  /**
   * Static method - BulkWrite operations to database in unordered way
   * 
   * @param {string} collectionName Collection name 
   * @param {Array} docs BulkWrite object array
   * @param {string} connString Database connection string 
   * @returns {promise}
   */
  static async allBulkUnOrdered(collectionName, docs, connString) { return Promise.resolve(await MongoDBOps.writeBulkData("allBulk", collectionName, docs, false, connString)); }
  
  /**
   * Instance method - BulkWrite operations to database in unordered way
   * 
   * @param {Array} docs BulkWrite object array
   * @returns {promise}
   */
  async allBulkUnOrdered(docs) { return Promise.resolve(await super.writeBulkData("allBulk", this.collectionName, docs, false, this.connString)); }
}

module.exports = MongoDB;