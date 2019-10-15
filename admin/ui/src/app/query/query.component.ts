import { Component, OnInit, ViewChild } from '@angular/core';
import { FormGroup, Validators, FormControl } from '@angular/forms';
import { ApiService } from '../services/api.service';
import { first } from 'rxjs/operators';

@Component({
  selector: 'app-query',
  templateUrl: './query.component.html',
  styleUrls: ['./query.component.scss']
})
export class QueryComponent implements OnInit {
  public queryForm: FormGroup;
  public code: string;
  public response: string;

  constructor(public api: ApiService) { }

  ngOnInit() {
    this.queryForm = new FormGroup({
      query: new FormControl('', [Validators.required, Validators.maxLength(1000)])
    });
  }

  public executeQuery() {
    this.api.query(this.code).pipe(first()).subscribe(res => {
      this.response = JSON.stringify(res);
    });
  }

  public clearEditor() {
    this.code = "";
    this.queryForm.controls.query.reset();
  }

  onKeyUp(event: any) {
    this.code = event.target.value;
  }
}
