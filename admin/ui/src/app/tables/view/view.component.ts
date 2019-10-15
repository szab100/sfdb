import { Component, OnInit, OnDestroy, ViewChild } from '@angular/core';
import { ActivatedRoute } from '@angular/router';
import { ApiService, Table } from 'src/app/services/api.service';
import { Subject, Observable } from 'rxjs';
import { first } from 'rxjs/operators';
import { MatTableDataSource } from '@angular/material/table';
import { DataSource } from '@angular/cdk/table';
import { MatPaginator } from '@angular/material/paginator';
import { MatSort } from '@angular/material/sort';

@Component({
  selector: 'app-view',
  templateUrl: './view.component.html',
  styleUrls: ['./view.component.scss']
})
export class ViewTableComponent implements OnInit, OnDestroy {

  @ViewChild(MatPaginator) paginator: MatPaginator;
  @ViewChild(MatSort) sort: MatSort;

  private paramSub: any;
  private tableName: string;
  private tableData: any = [];
  displayedColumns: string[];

  constructor(private api: ApiService, private route: ActivatedRoute) { }

  ngOnInit() {
    this.paramSub = this.route.params.subscribe(params => {
      this.tableName = params['name'];

      this.api.getTable(this.tableName).pipe(first()).subscribe(res => {
        this.displayedColumns = res.columns;
        var rows = [];
        res.rows.forEach(row => {
          var actualRow = [];
          res.columns.forEach((column, colindex) => {
            actualRow[column] = row[colindex];
          });
          rows.push(actualRow);
        });

        this.tableData = new MatTableDataSource(rows);
        this.tableData.paginator = this.paginator;
        this.tableData.sort = this.sort;
      });
   });
  }

  ngOnDestroy() {
    this.paramSub.unsubscribe();
  }

  applyFilter(filterValue: string) {
    this.tableData.filter = filterValue.trim().toLowerCase();

    if (this.tableData.paginator) {
      this.tableData.paginator.firstPage();
    }
  }
}


