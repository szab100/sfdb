import { Component, OnInit } from '@angular/core';
import { ApiService } from '../services/api.service';
import { first } from 'rxjs/operators';
import { MatTableDataSource } from '@angular/material/table';

@Component({
  selector: 'app-tables',
  templateUrl: './tables.component.html',
  styleUrls: ['./tables.component.scss']
})
export class TablesComponent implements OnInit {

  displayedColumns: string[] = ['name', 'actions'];
  tables: MatTableDataSource<String> = null;

  constructor(private api: ApiService) { }

  ngOnInit() {
    this.getTables();
  }

  refreshTables() {
    this.getTables();
  }

  private getTables() {
    this.api.getTables().pipe(first()).subscribe(res => {this.tables = new MatTableDataSource(res)});
  }
}
