import { RestgooseConnector, RestgooseModel, RestRequest, RestRegistry, RestError,
    ERROR_BAD_FORMAT_CODE, ERROR_NOT_FOUND_CODE, ERROR_VALIDATION_CODE } from '@xureilab/restgoose';
import * as mysql from 'mysql';
import { Connection } from 'mysql';

type Constructor<T> = new(...args: any[]) => T;
export interface Dic {
    [key: string]: any;
}

export const isPrimitive = Type => !!Type && ['ObjectId', 'ObjectID', 'String', 'Number', 'Boolean', 'Date', 'Decimal128'].find(n => Type.name === n);
export const isArray = Type => !!Type && Type.name === 'Array';
export const isObject = Type => {
    let prototype = Type.prototype;
    let name = Type.name;
    while (name) {
        if (name === 'String') {
            return false;
        }
        else if (name === 'Object') {
            return true;
        }
        prototype = Object.getPrototypeOf(prototype);
        name = prototype ? prototype.constructor.name : null;
    }
    return false;
};
export const isObjectLitteral = Type => {
    const name = Type.name;
    return (name === 'Object');
};
export const isNumber = Type => !!Type && Type.name === 'Number';
export const isString = Type => !!Type && Type.name === 'String';
export const isBoolean = Type => !!Type && Type.name === 'Boolean';
export const isDate = Type => !!Type && Type.name === 'Date';

type SQLDoc = {
    id: any;
    __created: boolean;
};

function buildWhere(req: RestRequest): String {
    // TODO Sanitization tests
    const restgooseReq = req.restgoose || {};
    if (!restgooseReq.query) {
        return 'TRUE';
    }
    else {
        //TODO
        return 'TRUE';
    }
}

function buildOneWhere(req: RestRequest, useFilter: boolean): String {
    const query = !useFilter ? 'TRUE' : buildWhere(req);
    if (req.params && req.params.id) {
        const id = parseInt(req.params.id);
        if (!Number.isInteger(id)) {
            throw new RestError(404, { code: ERROR_NOT_FOUND_CODE });
        }
        else {
            return `(${query}) AND (id=${id})`;
        }
    }
    else {
        return query;
    }
}

function flatten(value: any): String {
    if (Array.isArray(value) || value instanceof Object) {
        return `"${JSON.stringify(value).replace(/"/g, '\\\"')}"`;
    }
    else {
        return JSON.stringify(value);
    }
}

function unflatten<T extends RestgooseModel>(modelType: Constructor<T>, entity: T): any {
    const props = RestRegistry.listPropertiesOf(modelType);
    for (const prop of props) {
        if (entity[prop.name]) {
            if (Array.isArray(prop.type)) {
                if (isArray(prop.type[0]) || isObject(prop.type[0])) {
                    entity[prop.name] = JSON.parse(entity[prop.name]);
                }
            }
            else if (isArray(prop.type) || isObject(prop.type)) {
                entity[prop.name] = JSON.parse(entity[prop.name]);
            }
        }
    }
}

function buildSet<T extends RestgooseModel>(modelType: Constructor<T>, entity: T): String {
    // TODO Sanitization tests
    const props = RestRegistry.listPropertiesOf(modelType);
    const set = [];
    for (const prop of props) {
        if (!prop.config) {
            // TODO create a specific error class for Restgoose init errors
            throw new Error(`In ${name}: Property '${prop.name}' is missing a configuration. You probably forgot to add @prop() on it.`);
        }

        if (typeof(entity[prop.name]) !== 'undefined') {
            set.push(`\`${prop.name}\`=${flatten(entity[prop.name])}`);
        }
    }
    return set.join(', ');
    /*if (!restgooseReq.query) {
        return 'TRUE';
    }
    else {
        //TODO
        return 'TRUE';
    }*/
}

export class RestgooseSqlConnector implements RestgooseConnector {
    connection: Connection;

    constructor(connection: Connection) {
        this.connection = connection;
    }

    query(queryString: String): Promise<any> {
        return new Promise((resolve, reject) => {
            this.connection.query(queryString, function (error, results) {
                if (error) {
                    reject(error);
                }
                else {
                    resolve(results);
                }
            });
        });
    }
    queryOne(queryString: String): Promise<any> {
        return (
            this.query(queryString)
            .then(results => {
                if (results.length === 0) {
                    throw new RestError(404, { code: ERROR_NOT_FOUND_CODE });
                }
                else {
                    return results[0];
                }
            })
        );
    }

    async findOne<T extends RestgooseModel> (modelType: Constructor<T>, req: RestRequest, useFilter: boolean): Promise<T> {
        const where = buildOneWhere(req, useFilter);
        try {
            return (
                this.queryOne(`SELECT * FROM ${modelType.name} WHERE ${where} LIMIT 0,1`)
                .then(entity => {
                    unflatten(modelType, entity);
                    return entity;
                })
                .catch(e => handleError(e))
            );
        }
        catch (e) {
            handleError(e);
        }
    }
    async find<T extends RestgooseModel> (modelType: Constructor<T>, req: RestRequest): Promise<T[]> {
        try {
            const where = buildWhere(req);
            return (
                this.query(`SELECT * FROM ${modelType.name} WHERE ${where}`)
                .then((entities) => {
                    for (const e of entities) {
                        unflatten(modelType, e);
                    }
                    return entities;
                })
                .catch(e => handleError(e))
            );
        }
        catch (e) {
            handleError(e);
        }
    }
    async deleteOne <T extends RestgooseModel> (modelType: Constructor<T>, entity: T): Promise<boolean> {
        try {
            const sqlEntity = (entity as T & SQLDoc);
            return (
                this.query(`DELETE FROM ${modelType.name} WHERE id=${sqlEntity.id}`)
                .then(() => true)
                .catch(e => handleError(e))
            );
        }
        catch (e) {
            handleError(e);
        }
    }
    async delete <T extends RestgooseModel> (modelType: Constructor<T>, entities: T[]): Promise<boolean> {
        try {
            if (entities.length === 0) {
                return Promise.resolve(true);
            }
            else {
                const sqlEntities = (entities as Array<T & SQLDoc>);
                const where = `id IN (${sqlEntities.map(e => e.id).join(', ')})`;
                return (
                    this.query(`DELETE FROM ${modelType.name} WHERE ${where}`)
                    .then(() => true)
                    .catch(e => handleError(e))
                );
            }
        }
        catch (e) {
            handleError(e);
        }
    }
    async create <T extends RestgooseModel> (modelType: Constructor<T>, req: RestRequest): Promise<T> {
        try {
            const out = new modelType() as (T & SQLDoc);
            return Promise.resolve(out);
        }
        catch (e) {
            handleError(e);
        }
    }
    async save <T extends RestgooseModel> (modelType: Constructor<T>, entity: T): Promise<T> {
        const sqlEntity = (entity as T & SQLDoc);
        try {
            const set = buildSet(modelType, entity);
            if (!sqlEntity.id) {
                return (
                    this.query(`INSERT ${modelType.name} ${set === '' ? 'VALUES()' : 'SET '}${set}`)
                    .then((results) => {
                        sqlEntity.id = results.insertId;
                        return sqlEntity;
                    })
                    .catch(e => handleError(e))
                );
            }
            else {
                return (
                    this.query(`UPDATE ${modelType.name} SET ${set} WHERE id=${sqlEntity.id}`)
                    .then(() => sqlEntity)
                    .catch(e => handleError(e))
                );
            }
        }
        catch (e) {
            handleError(e);
        }
    }
}

function handleError<T extends RestgooseModel>(error): T {
    if (error instanceof RestError) {
        throw error;
    }
    else if (error.errno === 1364) { //ER_NO_DEFAULT_FOR_FIELD
        throw new RestError(400);
    }
    else {
        throw new RestError(500);
    }
}
