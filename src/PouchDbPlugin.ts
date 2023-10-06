import PouchDB from 'pouchdb';
import { IDbPlugin, IDbPluginOptions, IBulkOperationsResponse, IBulkOperation, IQueryParams } from '@agrejus/db-framework';
import findAdapter from 'pouchdb-find';
import memoryAdapter from 'pouchdb-adapter-memory';
import { validateAttachedEntity } from './validator';
import { PouchDbRecord } from './types';

PouchDB.plugin(findAdapter);
PouchDB.plugin(memoryAdapter);

export class PouchDbPlugin<TDocumentType extends string, TEntityBase extends PouchDbRecord<TDocumentType>, TDbPluginOptions extends IDbPluginOptions = IDbPluginOptions> implements IDbPlugin<TDocumentType, TEntityBase, "_id" | "_rev"> {

    protected readonly options: TDbPluginOptions;
    readonly idPropertName = "_id";

    readonly types = {
        exclusions: "" as "_id" | "_rev"
    }

    constructor(options: TDbPluginOptions) {
        this.options = options;
    }

    protected createDb() {
        const { dbName, ...options } = this.options
        return new PouchDB<TEntityBase>(this.options.dbName, options);
    }

    async doWork<T>(action: (db: PouchDB.Database<TEntityBase>) => Promise<T>, shouldClose: boolean = true) {
        const db = this.createDb();
        const result = await action(db);

        if (shouldClose) {
            await db.close();
        }

        return result;
    }

    async destroy() {
        return await this.doWork(w => w.destroy(), false);
    }

    async all(payload?: IQueryParams<TDocumentType>) {
        const result = await this.doWork(w => {
            try {
                const findOptions: PouchDB.Find.FindRequest<TEntityBase> = {
                    selector: {},
                }

                if (payload != null) {
                    findOptions.selector = payload
                }

                return w.find(findOptions) as Promise<PouchDB.Find.FindResponse<TEntityBase>>
            } catch (e) {

                if ('message' in e && e.message.includes("database is closed")) {
                    throw e;
                }

                return Promise.resolve<PouchDB.Find.FindResponse<TEntityBase>>({
                    docs: []
                });
            }
        });

        return result.docs as TEntityBase[];
    }

    async query(request: PouchDB.Find.FindRequest<TEntityBase>) {
        return await this.doWork(w => w.find(request))
    }

    async getStrict(...ids: string[]) {
        if (ids.length === 0) {
            return [];
        }

        const result = await this.doWork(w => w.bulkGet({ docs: ids.map(x => ({ id: x })) }));

        return result.results.map(w => {
            const result = w.docs[0];

            if ('error' in result) {
                throw new Error(`docid: ${w.id}, error: ${JSON.stringify(result.error, null, 2)}`)
            }

            return result.ok as TEntityBase;
        });
    }

    async get(...ids: string[]) {
        try {

            const result = await this.doWork(w => w.find({
                selector: {
                    _id: {
                        $in: ids
                    }
                }
            }), false);

            return result.docs as TEntityBase[];
        } catch (e) {

            if ('message' in e && e.message.includes("database is closed")) {
                throw e;
            }

            return [] as TEntityBase[];
        }
    }

    async bulkOperations(operations: { adds: TEntityBase[]; removes: TEntityBase[]; updates: TEntityBase[]; }) {
        const { adds, removes, updates } = operations;
        const response = await this.doWork(w => w.bulkDocs([...removes, ...adds, ...updates]));

        return this.formatBulkDocsResponse(response)
    }

    protected formatBulkDocsResponse(response: (PouchDB.Core.Response | PouchDB.Core.Error)[]) {
        const result: IBulkOperationsResponse = {
            errors: {},
            successes: {},
            errors_count: 0,
            successes_count: 0
        };

        for (let item of response) {
            if ('error' in item) {
                const error = item as PouchDB.Core.Error;

                if (!error.id) {
                    continue;
                }

                result.errors_count += 1;
                result.errors[error.id] = {
                    id: error.id,
                    ok: false,
                    error: error.message,
                    rev: error.rev
                } as IBulkOperation;
                continue;
            }

            const success = item as PouchDB.Core.Response;

            result.successes_count += 1;
            result.successes[success.id] = {
                id: success.id,
                ok: success.ok,
                rev: success.rev
            } as IBulkOperation;
        }

        return result;
    }

    async prepareAttachments(...entities: TEntityBase[]) {
        const validationFailures = entities.map(w => validateAttachedEntity<TDocumentType, TEntityBase>(w)).flat().filter(w => w.ok === false);
        const result: { ok: boolean, docs: TEntityBase[], errors: string[] } = {
            ok: true,
            docs: [],
            errors: []
        }

        if (validationFailures.length > 0) {
            result.errors = validationFailures.map(w => w.error);
            result.ok = false;
            return result;
        }

        const found = await this.getStrict(...entities.map(w => w._id));
        const foundDictionary = found.reduce((a, v) => ({ ...a, [v._id]: v._rev }), {} as { [key: string]: any });
        result.docs = entities.map(w => ({ ...w, _rev: foundDictionary[w._id] } as TEntityBase));

        return result;
    }

    private _isAdditionAllowed(entity: TEntityBase) {
        const indexableEntity = entity as any;

        // cannot add an entity that already has a rev, means its in the database already
        if (!!indexableEntity["_rev"]) {
            return false
        }

        return true;
    }

    formatDeletions(...entities: TEntityBase[]): TEntityBase[] {
        return entities.map(w => {

            let result = { ...w, _id: w._id, _rev: w._rev, DocumentType: w.DocumentType, _deleted: true } as any;

            return result as TEntityBase
        })
    }

    isOperationAllowed(entity: TEntityBase, operation: 'add') {

        if (operation === "add") {
            return this._isAdditionAllowed(entity);
        }

        return false
    }

    async prepareAdditions(...entities: TEntityBase[]) {
        const result: { ok: boolean, docs: TEntityBase[], errors: string[] } = {
            ok: true,
            docs: [],
            errors: []
        }

        for (const entity of entities) {
            if (!!entity["_rev"]) {
                result.errors.push('Cannot add entity that is already in the database, please modify entites by reference or attach an existing entity');
                result.ok = false;
            }
        }

        if (result.ok === false) {
            return result;
        }

        result.docs = entities;
        return result;
    }

    prepareDetachments(...entities: TEntityBase[]): { ok: boolean; errors: string[]; docs: TEntityBase[]; } {
        const validationFailures = entities.map(w => validateAttachedEntity<TDocumentType, TEntityBase>(w)).flat().filter(w => w.ok === false);
        const result: { ok: boolean, docs: TEntityBase[], errors: string[] } = {
            ok: true,
            docs: [],
            errors: []
        }

        if (validationFailures.length > 0) {
            result.errors = validationFailures.map(w => w.error);
            result.ok = false;
            return result;
        }

        result.docs = entities;
        return result;
    }

    setDbGeneratedValues(response: IBulkOperationsResponse, entities: TEntityBase[]): void {
        for (let i = 0; i < entities.length; i++) {
            const modification = entities[i];
            const found = response.successes[modification._id];

            // update the rev in case we edit the record again
            if (found && found.ok === true) {
                const indexableEntity = modification as any;
                indexableEntity._rev = found.rev;
            }
        }
    }
}