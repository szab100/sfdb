import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { DropTableDialogComponent } from './drop.component';

describe('DropTableDialogComponent', () => {
  let component: DropTableDialogComponent;
  let fixture: ComponentFixture<DropTableDialogComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ DropTableDialogComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(DropTableDialogComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
