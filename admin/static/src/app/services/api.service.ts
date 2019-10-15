import { Injectable } from '@angular/core';
import { HttpClient, HttpHeaders } from '@angular/common/http';
import { Observable, throwError, Subscription, timer, interval } from 'rxjs';
import { retry, catchError, map, first } from 'rxjs/operators';

import { environment } from '../../environments/environment';

@Injectable({
    providedIn: 'root'
})
export class ApiService {
    public static DEFAULT_DB_NAME = 'MAIN';
    public static DEFAULT_TTL = 60;

    private static REFRESH_INTERVAL = 1;
    private connected: boolean = null;
    private connectionCheckerTimer: Subscription = null;

    baseUrl = environment['API_ENDPOINT'];
    httpOptions = {
        headers: new HttpHeaders({
            'Content-Type': 'application/json'
        })
    }

    constructor(private http: HttpClient) {
        // Check if we are already connected
        if (this.connected == null) {
            this.ping().pipe(first()).subscribe(() => {
                if (this.connected) {
                    this.setConnectionCheckerTimer();
                }
            });
        }
    }

    public connect(host: string, port: number, user: string, password: string, ttl?: number, db?: string): Observable<any> {
        db = db ? db : ApiService.DEFAULT_DB_NAME;
        ttl = ttl ? ttl : ApiService.DEFAULT_TTL;
        const params = <ConnectionArg>{
            conn_str: user + ":" + password + "@" + host + ":" + port.toString() + "/" + db + "?" + "ttl=" + ttl.toString()
        }

        return this.http.post<any>(this.baseUrl + '/connect', JSON.stringify(params), this.httpOptions).pipe(map(
            res => {
                this.connected = true;
                this.setConnectionCheckerTimer();
                return res;
            }
        ));
    }

    public ping(): Observable<any> {
        return this.http.get<any>(this.baseUrl + '/ping', this.httpOptions).pipe(map(
            res => {
                this.connected = true;
                return res;
            }), catchError(err => {
                this.connected = false;
                if (this.connectionCheckerTimer != null && !this.connectionCheckerTimer.closed) {
                    this.connectionCheckerTimer.unsubscribe();
                }
                return err;
            })
        );
    }

    public disconnect(): Observable<any> {
        return this.http.post<any>(this.baseUrl + '/close', null, this.httpOptions).pipe(
            map((res: Response) => {
                this.connected = false;
                if (this.connectionCheckerTimer != null && !this.connectionCheckerTimer.closed) {
                    this.connectionCheckerTimer.unsubscribe();
                }
            }));
    }

    public isConnected(): boolean {
        return this.connected;
    }

    public getTables(db?: string): Observable<any> {
        db = db ? db : ApiService.DEFAULT_DB_NAME;
        return this.http.get<any>(this.baseUrl + '/' + db, this.httpOptions);
    }

    public getTable(table: string, db?: string): Observable<any> {
        db = db ? db : ApiService.DEFAULT_DB_NAME;
        return this.http.get<any>(this.baseUrl + '/' + db + '/' + table, this.httpOptions);
    }

    public createTable(table: string, fields: TableField[], db?: string): Observable<any> {
        const params = <CreateTableArg>{
            fields: fields
        }
        return this.http.post<any>(this.baseUrl + '/' + db + '/' + table, JSON.stringify(params), this.httpOptions);
    }

    public deleteTable(table: string, db?: string): Observable<any> {
        db = db ? db : ApiService.DEFAULT_DB_NAME;
        return this.http.delete<any>(this.baseUrl + '/' + db + '/' + table, this.httpOptions);
    }

    public query(query: string, db?: string): Observable<any> {
        db = db ? db : ApiService.DEFAULT_DB_NAME;
        return this.http.put<any>(this.baseUrl + '/' + db, JSON.stringify(query), this.httpOptions);
    }

    private setConnectionCheckerTimer() {
        if (this.isConnected()) {
            this.connectionCheckerTimer = interval(ApiService.REFRESH_INTERVAL * 1000).subscribe(
                () => {
                    this.ping().subscribe();
                }
            );
        }
    }
}

export interface ConnectionArg {
    conn_str: string
}

export interface TableField {
    name: string,
    field_type: string
}

export interface CreateTableArg {
    fields: TableField[]
}

export interface Table {
    columns: string[],
    rows: any[][],
    column_types: string[][]
}