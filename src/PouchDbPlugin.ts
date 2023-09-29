import PouchDB from 'pouchdb';
import { IDbPlugin, IDbPluginOptions, IBulkOperationsResponse, IBulkOperation, IDbRecord, IQueryParams } from '@agrejus/db-framework';
import findAdapter from 'pouchdb-find';
import memoryAdapter from 'pouchdb-adapter-memory';

PouchDB.plugin(findAdapter);
PouchDB.plugin(memoryAdapter);

export class PouchDbPlugin<TDocumentType extends string, TEntityBase extends IDbRecord<TDocumentType>, TDbPluginOptions extends IDbPluginOptions = IDbPluginOptions> implements IDbPlugin<TDocumentType, TEntityBase> {

    private readonly _options: TDbPluginOptions;

    constructor(options: TDbPluginOptions) {
        this._options = options;
    }

    protected createDb() {
        const { dbName, ...options } = this._options
        return new PouchDB<TEntityBase>(this._options.dbName, options);
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
        return await this.doWork(async w => await w.destroy(), false);
    }

    async all(payload?: IQueryParams<TDocumentType>) {
        return await this.doWork(async w => {
            try {
                const findOptions: PouchDB.Find.FindRequest<TEntityBase> = {
                    selector: {},
                }

                if (payload != null) {
                    findOptions.selector = payload
                }

                const result = await w.find(findOptions)

                return result.docs as TEntityBase[];
            } catch (e) {

                if ('message' in e && e.message.includes("database is closed")) {
                    throw e;
                }

                return [] as TEntityBase[];
            }
        })
    }

    async query(request: PouchDB.Find.FindRequest<TEntityBase>) {
        return await this.doWork(async w => await w.find(request))
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
}