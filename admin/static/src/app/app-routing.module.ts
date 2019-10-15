import { NgModule } from '@angular/core';
import { Routes, RouterModule } from '@angular/router';

import { HomeComponent } from './home/home.component';
import { ConnectionComponent } from './connection/connection.component';
import { TablesComponent } from './tables/tables.component';
import { StatsComponent } from './stats/stats.component';
import { ErrorComponent } from './error/error.component';
import { ViewTableComponent as ViewTableComponent } from './tables/view/view.component';
import { CreateTableComponent } from './tables/create/create.component';
import { DescribeTableComponent } from './tables/describe/describe.component';
import { QueryComponent } from './query/query.component';

const routes: Routes = [
  { path: 'home', component: HomeComponent, /* canActivate: [AuthService] */ },
  { path: 'connection', component: ConnectionComponent, /* canActivate: [AuthService] */ },
  { path: 'tables', component: TablesComponent, /* canActivate: [AuthService] */ },
  { path: 'tables/:name/view', component: ViewTableComponent, /* canActivate: [AuthService] */ },
  { path: 'tables/:name/describe', component: DescribeTableComponent, /* canActivate: [AuthService] */ },
  { path: 'tables/create', component: CreateTableComponent, /* canActivate: [AuthService] */ },
  { path: 'query', component: QueryComponent, /* canActivate: [AuthService] */ },
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
