import { NgModule } from '@angular/core';
import { Routes, RouterModule } from '@angular/router';

import { HomeComponent } from './home/home.component';
import { ConnectionComponent } from './connection/connection.component';
import { TablesComponent } from './tables/tables.component';
import { StatsComponent } from './stats/stats.component';
import { ErrorComponent } from './error/error.component';

const routes: Routes = [
  { path: 'home', component: HomeComponent, /* canActivate: [AuthService] */ },
  { path: 'connection', component: ConnectionComponent, /* canActivate: [AuthService] */ },
  { path: 'tables', component: TablesComponent, /* canActivate: [AuthService] */ },
  { path: 'stats', component: StatsComponent, /* canActivate: [AuthService] */ },
  { path: 'error', component: ErrorComponent },

  {
    path: '',
    redirectTo: '/home',
    pathMatch: 'full'
  }
];

@NgModule({
  imports: [RouterModule.forRoot(routes)],
  exports: [RouterModule]
})
export class AppRoutingModule { }
