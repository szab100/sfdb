import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { CreateTableComponent } from './create.component';

describe('CreateTableComponent', () => {
  let component: CreateTableComponent;
  let fixture: ComponentFixture<CreateTableComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ CreateTableComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(CreateTableComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
