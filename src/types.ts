import { IDbRecord } from "@agrejus/db-framework";

export type PouchDbRecord<TDocumentType extends string> = {
    readonly _id: string;
    readonly _rev: string;
} & IDbRecord<TDocumentType>