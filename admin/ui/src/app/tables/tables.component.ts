import { Component, OnInit } from '@angular/core';
import { ApiService } from '../services/api.service';
import { first } from 'rxjs/operators';
import { MatTableDataSource } from '@angular/material/table';
import { MatDialog } from '@angular/material/dialog';
import { DropTableDialogComponent } from './drop/drop.component';
import { MatSnackBar } from '@angular/material/snack-bar';

@Component({
  selector: 'app-tables',
  templateUrl: './tables.component.html',
  styleUrls: ['./tables.component.scss']
})
export class TablesComponent implements OnInit {

  displayedColumns: string[] = ['name', 'actions'];
  tables: MatTableDataSource<String> = null;

  constructor(private api: ApiService, public dialog: MatDialog, private snackBar: MatSnackBar) { }

  ngOnInit() {
    this.getTables();
  }

  refreshTables() {
    this.getTables();
  }

  deleteTable(table: string) {
    const dialogRef = this.dialog.open(DropTableDialogComponent, {
      width: '300px',
      data: {table: table}
    });

    dialogRef.afterClosed().subscribe(result => {
      if (result == "yes") {
        this.api.query("DROP TABLE LoadTest;").pipe(first()).subscribe(res => {
          this.snackBar.openFromComponent(TableDropConfirmation, {
            duration: 2 * 1000,
          });
          this.refreshTables();
        })
      }
    });
  }

  private getTables() {
    this.api.getTables().pipe(first()).subscribe(res => {this.tables = new MatTableDataSource(res)});
  }
}

@Component({
  selector: 'snack-bar-component-example-snack',
  template: '<span class="snackBar confirmSnackBar">Table deleted successfully!</span>',
  styles: [`
    .confirmSnackBar {
      color: green;
    }
  `],
})
export class TableDropConfirmation {}

export interface DialogData {
  table: string;
}