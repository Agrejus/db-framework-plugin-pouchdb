import { PouchDbRecord } from './types';

export class Transaction<TDocumentType extends string, TEntityBase extends PouchDbRecord<TDocumentType>, TResult> {

    private readonly _resolve: (value: { result: TResult; db: PouchDB.Database<TEntityBase>; } | PromiseLike<{ result: TResult; db: PouchDB.Database<TEntityBase>; }>) => void;
    private readonly _reject: (reason?: any) => void;
    private readonly _action: (db: PouchDB.Database<TEntityBase>) => Promise<TResult>;
    private readonly _createDb: () => PouchDB.Database<TEntityBase>;
    private _backoffTimeout: number = 0;
    private readonly _maxBackoffTimeout: number = 2000;

    constructor(
        resolve: (value: { result: TResult; db: PouchDB.Database<TEntityBase>; } | PromiseLike<{ result: TResult; db: PouchDB.Database<TEntityBase>; }>) => void,
        reject: (reason?: any) => void,
        action: (db: PouchDB.Database<TEntityBase>) => Promise<TResult>,
        createDb: () => PouchDB.Database<TEntityBase>
    ) {
        this._resolve = resolve;
        this._reject = reject;
        this._action = action;
        this._createDb = createDb;
    }

    private _shouldRetry(error: any) {

        if ('status' in error && typeof error.status === "number") {

            const status: number = error.status;

            return status >= 500;
        }

        return false
    }

    async execute() {

        try {
            const db = this._createDb();
            const result = await this._action(db);

            this._resolve({ result, db })
        } catch (e: any) {
            if (this._shouldRetry(e) === true) {

                if (this._backoffTimeout === 0) {
                    this._backoffTimeout = 25;
                } else {
                    this._backoffTimeout = this._backoffTimeout * 2;
                }

                if (this._backoffTimeout >= this._maxBackoffTimeout) {

                    if ('message' in e && typeof e.message === 'string') {
                        this._reject({ ...e, message: `Retry Failed.  Max Wait: ${this._backoffTimeout}.  Original Message: ${e.message}` })
                        return
                    }

                    this._reject(e)
                    return;
                }

                const callback = this.execute.bind(this);
                setTimeout(() => callback(this._action), this._backoffTimeout);
                return;
            }

            this._reject(e);
        }
    }
}