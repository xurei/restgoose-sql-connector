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
        if (name === 'Object') {
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

/*export async function getModel<T extends RestgooseModel>(modelEntry: RestModelEntry<T>, req: RestRequest): Promise<Model<T & Document>> {
    // FIXME as any
    const connection = modelEntry.restConfig.getConnection ? await modelEntry.restConfig.getConnection(req) as any : mongoose;
    const model = modelEntry.type;

    return getModelForConnection(model, connection);
}*/

// const schemas = {};
// function buildSchema<T extends RestgooseModel>(modelType: Constructor<T>, schemaOptions?) {
//     const name = modelType.name;
//     if (schemas[name]) {
//         return schemas[name];
//     }
//
//     let sch: mongoose.Schema;
//     const parentCtor = Object.getPrototypeOf(modelType);
//     if (parentCtor && parentCtor.name !== 'RestgooseModel' && parentCtor.name !== 'Object') {
//         const parentSchema = buildSchema(parentCtor, schemaOptions);
//         sch = parentSchema.clone();
//     }
//     else {
//         sch = schemaOptions ? new mongoose.Schema({}, schemaOptions) : new mongoose.Schema({});
//     }
//
//     const props = RestRegistry.listPropertiesOf(modelType as Constructor<RestgooseModel>);
//     for (const prop of props) {
//         if (!prop.config) {
//             // TODO create a specific error class for Restgoose init errors
//             throw new Error(`In ${name}: Property '${prop.name}' is missing a configuration. You probably forgot to add @prop() on it.`);
//         }
//
//         const config: Dic = {
//             required: prop.config.required || false,
//             index: prop.config.index || false,
//             unique: prop.config.unique || false,
//             default: prop.config.default,
//         };
//         if (prop.config.validate) {
//             config.validate = prop.config.validate;
//         }
//         if (prop.config.enum) {
//             if (typeof(prop.config.enum) === 'object') {
//                 config.enum = Object.keys(prop.config.enum).map(k => prop.config.enum[k]);
//             }
//             else {
//                 throw new Error(`In ${name}: Option 'enum' must be an array, object litteral, or enum type`);
//             }
//         }
//
//         if (Array.isArray(prop.type)) {
//             if (isPrimitive(prop.type[0])) {
//                 config.type = prop.type;
//             }
//             else if ((prop.config as any).ref === true) {
//                 config.type = [mongoose.Schema.Types.ObjectId];
//             }
//             else {
//                 const Type = prop.type[0] as Constructor<RestgooseModel>;
//                 const subSchema = buildSchema(Type); //No schemaOptions ??
//                 config.type = [subSchema];
//             }
//         }
//         else if (!isPrimitive(prop.type) && !isArray(prop.type) && isObject(prop.type)) {
//             if (isObjectLitteral(prop.type)) {
//                 config.type = Object;
//             }
//             else {
//                 const Type = prop.type as Constructor<RestgooseModel>;
//                 config.type = buildSchema(Type); //No schemaOptions ??
//             }
//         }
//         else {
//             config.type = prop.type;
//         }
//
//         const s = {};
//         s[prop.name] = config;
//         sch.add(s);
//     }
//
//     /*const indices = Reflect.getMetadata('typegoose:indices', t) || [];
//     for (const index of indices) {
//         sch.index(index.fields, index.options);
//     }*/
//
//     schemas[name] = sch;
//     return sch;
// }

function buildOneQuery(connection: Connection, req: RestRequest, useFilter: boolean) {
    connection.query('SELECT 1 + 1 AS solution', function (error, results, fields) {
      if (error) throw error;
      console.log('The solution is: ', results[0].solution);
    });

    const restgooseReq = req.restgoose || {};
    const query = !useFilter ? {} : ( restgooseReq.query || {} );
    if (req.params && req.params.id) {
        return { $and: [
            { _id: req.params.id },
            query,
        ]} as any;
    }
    else {
        return query as any;
    }
}

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
        return `(${query}) AND (id=${req.params.id})`;
    }
    else {
        return query;
    }
}

function flatten<T extends RestgooseModel>(value: any): String {
    if (Array.isArray(value) || value instanceof Object) {
        return `"${JSON.stringify(value).replace(/"/g, '\\\"')}"`;
    }
    else {
        return JSON.stringify(value);
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

        set.push(`\`${prop.name}\`=${flatten(entity[prop.name])}`);
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

    query(queryString: String): Promise<Array<any>> {
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
                    throw new RestError(404);
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
            return this.queryOne(`SELECT * FROM ${modelType.name} WHERE ${where} LIMIT 0,1`);
        }
        catch (e) {
            handleError(e);
        }
    }
    async find<T extends RestgooseModel> (modelType: Constructor<T>, req: RestRequest): Promise<T[]> {
        try {
            const where = buildWhere(req);
            return this.connection.query(`SELECT * FROM ${modelType.name} WHERE ${where}`);
        }
        catch (e) {
            handleError(e);
        }
    }
    async deleteOne <T extends RestgooseModel> (modelType: Constructor<T>, req: RestRequest): Promise<boolean> {
        const where = buildOneWhere(req, true);
        try {
            return this.connection.query(`DELETE FROM ${modelType.name} WHERE ${where} LIMIT 0,1`);
        }
        catch (e) {
            handleError(e);
        }
    }
    async delete <T extends RestgooseModel> (modelType: Constructor<T>, req: RestRequest): Promise<boolean> {
        try {
            const where = buildWhere(req);
            return this.connection.query(`DELETE FROM ${modelType.name} WHERE ${where}`);
        }
        catch (e) {
            handleError(e);
        }
    }
    async create <T extends RestgooseModel> (modelType: Constructor<T>, req: RestRequest): Promise<T> {
        try {
            const out = new modelType() as (T & SQLDoc);
            out.__created = true;
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
            if (sqlEntity.__created) {
                return (
                    this.query(`LOCK TABLE ${modelType.name} WRITE`)
                    .then(() => this.query(`INSERT ${modelType.name} SET ${set}`))
                    .then(() => this.queryOne(`SELECT id FROM ${modelType.name} ORDER BY id DESC LIMIT 0,1`))
                    .then((result) => {
                        sqlEntity.id = result.id;
                    })
                    .then(() => this.query(`UNLOCK TABLES`))
                    .catch(e => {
                        return (
                            this.query(`UNLOCK TABLES`)
                            .then(() => handleError(e))
                        );
                    })
                    .then(() => sqlEntity)
                );
            }
            else {
                return new Promise((resolve, reject) => {
                    this.connection.query(`UPDATE ${modelType.name} WHERE id=${sqlEntity.id}`, function (error, results, fields) {
                        if (error) {
                            reject(error);
                        }
                        else {
                            resolve(entity);
                        }
                    });
                });
            }
        }
        catch (e) {
            handleError(e);
        }
    }
}

function handleError(error) {
    throw new RestError(500);
}
