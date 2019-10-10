import { BrowserModule } from '@angular/platform-browser';
import { NgModule } from '@angular/core';

import { AppRoutingModule } from './app-routing.module';
import { AppComponent } from './app.component';
import { BrowserAnimationsModule } from '@angular/platform-browser/animations';
import { HomeComponent } from './home/home.component';
import { HeaderComponent } from './header/header.component';
import { NavbarComponent } from './header/navbar/navbar.component';
import { SidenavComponent } from './header/sidenav/sidenav.component';
import { MatButtonModule, MatCardModule, MatIconModule, MatSidenavModule, MatRadioModule } from '@angular/material';
import { ErrorComponent } from './error/error.component';
import { ConnectionComponent } from './connection/connection.component';
import { TablesComponent } from './tables/tables.component';
import { StatsComponent } from './stats/stats.component';

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
    StatsComponent
  ],
  imports: [
    BrowserModule,
    AppRoutingModule,
    BrowserAnimationsModule,
    /* Angular Material */
    MatButtonModule,
    MatCardModule,
    MatIconModule,
    MatSidenavModule, MatRadioModule

  ],
  providers: [],
  bootstrap: [AppComponent]
})
export class AppModule { }
