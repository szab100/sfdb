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
    this.api.getTables().pipe(first()).subscribe(res => {console.log(res[0]);  this.tables = new MatTableDataSource(res)});
  }
}
