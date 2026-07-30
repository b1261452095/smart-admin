declare module 'sortablejs' {
  export type SortableEvent = {
    oldIndex?: number;
    newIndex?: number;
  };

  export type SortableOptions = {
    animation?: number;
    handle?: string;
    ghostClass?: string;
    chosenClass?: string;
    onEnd?: (event: SortableEvent) => void;
  };

  export default class Sortable {
    static create(element: HTMLElement, options?: SortableOptions): Sortable;
    destroy(): void;
  }
}
