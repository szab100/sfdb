import { BrowserModule } from '@angular/platform-browser';
import { NgModule } from '@angular/core';
import { AppRoutingModule } from './app-routing.module';
import { AppComponent } from './app.component';
import { BrowserAnimationsModule } from '@angular/platform-browser/animations';
import { HttpClientModule, HTTP_INTERCEPTORS } from '@angular/common/http';
import { FormsModule, ReactiveFormsModule } from '@angular/forms';
//import { FlexLayoutModule } from '@angular/flex-layout';
import {
  MatButtonModule, MatCardModule, MatIconModule, MatSidenavModule,
  MatRadioModule, MatInputModule, MatTableModule, MatPaginatorModule,
  MatSortModule, MatProgressSpinnerModule, MatDialogModule, MatSnackBarModule
} from '@angular/material';
import { HighlightModule } from 'ngx-highlightjs';
import sql from 'highlight.js/lib/languages/sql';

import { ApiService } from './services/api.service';
import { HttpErrorInterceptor } from './services/http-error.interceptor';

import { HomeComponent } from './home/home.component';
import { HeaderComponent } from './header/header.component';
import { NavbarComponent } from './header/navbar/navbar.component';
import { SidenavComponent } from './header/sidenav/sidenav.component';
import { ErrorComponent } from './error/error.component';
import { ConnectionComponent } from './connection/connection.component';
import { TablesComponent, TableDropConfirmation } from './tables/tables.component';
import { StatsComponent } from './stats/stats.component';
import { DisableControlDirective } from './misc/disableFormControl.directive';
import { DescribeTableComponent } from './tables/describe/describe.component';
import { ViewTableComponent } from './tables/view/view.component';
import { CreateTableComponent } from './tables/create/create.component';
import { QueryComponent } from './query/query.component';
import { DropTableDialogComponent } from './tables/drop/drop.component';

export function hljsLanguages() {
  return [{ name: 'sql', func: sql }];
}

@NgModule({
  declarations: [
    AppComponent,
    HomeComponent,
    HeaderComponent,
    NavbarComponent,
    SidenavComponent,
    ErrorComponent,
    ConnectionComponent,
    TablesComponent,
    StatsComponent,
    DisableControlDirective,
    DescribeTableComponent,
    ViewTableComponent,
    CreateTableComponent,
    QueryComponent,
    DropTableDialogComponent,
    TableDropConfirmation
  ],
  imports: [
    BrowserModule,
    AppRoutingModule,
    BrowserAnimationsModule,
    FormsModule,
    ReactiveFormsModule,
    HttpClientModule,
    //FlexLayoutModule,
    HighlightModule.forRoot({
      languages: hljsLanguages
    }),
    /* Angular Material */
    MatButtonModule,
    MatCardModule,
    MatIconModule,
    MatInputModule,
    MatSidenavModule,
    MatRadioModule,
    MatTableModule,
    MatPaginatorModule,
    MatSortModule,
    MatProgressSpinnerModule,
    MatDialogModule,
    MatSnackBarModule

  ],
  entryComponents: [
    DropTableDialogComponent,
    TableDropConfirmation
  ],
  providers: [
    {
      provide: HTTP_INTERCEPTORS,
      useClass: HttpErrorInterceptor,
      multi: true
    },
    ApiService
  ],
  bootstrap: [AppComponent]
})
export class AppModule { }
