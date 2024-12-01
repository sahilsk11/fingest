export interface ListImportRunUpdatesRequest {
    importRunId: string;
}

export interface ListImportRunUpdatesResponse {
    updates: ImportRunUpdate[];
}

export interface ImportRunUpdate {
    status: string;
    description?: string; // Optional since it can be null in Go
    updatedAt: string;
}
