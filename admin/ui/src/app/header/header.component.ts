import { Component, OnInit, AfterViewInit, ElementRef } from '@angular/core';
import { ApiService } from '../services/api.service';

@Component({
  selector: 'app-header',
  templateUrl: './header.component.html',
  styleUrls: ['./header.component.scss']
})
export class HeaderComponent implements OnInit, AfterViewInit {

  constructor(public api: ApiService) { }

  ngOnInit() {
  }

  ngAfterViewInit() {
  }
}
