import { Component, OnInit } from '@angular/core';
import { FormControl, FormGroup, Validators } from '@angular/forms';
import { ApiService } from '../services/api.service';

@Component({
  selector: 'app-connection',
  templateUrl: './connection.component.html',
  styleUrls: ['./connection.component.scss']
})
export class ConnectionComponent implements OnInit {
  public connectionForm: FormGroup;

  constructor(public api: ApiService) { }

  ngOnInit() {
    this.connectionForm = new FormGroup({
      hostname: new FormControl('localhost', [Validators.required, Validators.maxLength(60)]),
      port: new FormControl('27910', [Validators.required, Validators.min(1), Validators.max(65535)]),
      db: new FormControl('MAIN', [Validators.maxLength(20)]),
      ttl: new FormControl('5', [Validators.min(1), Validators.max(600)])
    });
  }

  public hasError = (controlName: string, errorName: string) => {
    return this.connectionForm.controls[controlName].hasError(errorName);
  }

  public connect() {
    this.api.connect(this.connectionForm.controls['hostname'].value, this.connectionForm.controls['port'].value,
      null, null, this.connectionForm.controls['ttl'].value, this.connectionForm.controls['db'].value).subscribe();
  }

  public disconnect() {
    this.api.disconnect().subscribe();
  }
}
