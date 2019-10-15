import { Component, OnInit, ViewChild } from '@angular/core';
import { MatPaginator, MatSort, MatTableDataSource } from '@angular/material';
import { ApiService, TableStructure } from 'src/app/services/api.service';
import { ActivatedRoute } from '@angular/router';
import { first } from 'rxjs/operators';

@Component({
  selector: 'app-describe',
  templateUrl: './describe.component.html',
  styleUrls: ['./describe.component.scss']
})
export class DescribeTableComponent implements OnInit {

  @ViewChild(MatPaginator) paginator: MatPaginator;
  @ViewChild(MatSort) sort: MatSort;

  private paramSub: any;
  public tableName: string;
  public tableData: any = [];
  displayedColumns: string[] = ["id", "name", "type"];

  constructor(public api: ApiService, private route: ActivatedRoute) { }

  ngOnInit() {
    this.paramSub = this.route.params.subscribe(params => {
      this.tableName = params['name'];

      this.api.describeTable(this.tableName).pipe(first()).subscribe(res => {
        var rows = [];
        res.fields.forEach((field, index) => {
          var row = {
            "id": index,
            "name": field.name,
            "type": field.field_type
          };
          rows.push(row);
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

}
