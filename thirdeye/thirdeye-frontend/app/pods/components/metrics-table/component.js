import Component from '@ember/component';
import { computed } from '@ember/object';
import { makeSortable, toMetricLabel, toColorDirection, isInverse } from 'thirdeye-frontend/helpers/utils';

export default Component.extend({
  /**
   * Columns for metrics table
   * @type Object[]
   */
  metricsTableColumns: [
    {
      template: 'custom/table-checkbox'
    }, {
      propertyName: 'label',
      title: 'Metric',
      className: 'rootcause-metric__table__column'
    }, {
      propertyName: 'score',
      title: 'Anomalous Score',
      disableFiltering: true,
      className: 'rootcause-metric__table__column'
    }, {
      propertyName: 'wo1w',
      template: 'custom/metrics-table-changes/wo1w',
      sortedBy: 'sortable_wo1w',
      title: 'WoW',
      disableFiltering: true,
      className: 'rootcause-metric__table__column'
    }, {
      propertyName: 'wo2w',
      template: 'custom/metrics-table-changes/wo2w',
      sortedBy: 'sortable_wo2w',
      title: 'Wo2W',
      disableFiltering: true,
      className: 'rootcause-metric__table__column'
    }, {
      propertyName: 'wo3w',
      template: 'custom/metrics-table-changes/wo3w',
      sortedBy: 'sortable_wo3w',
      title: 'Wo3W',
      disableFiltering: true,
      className: 'rootcause-metric__table__column'
    }, {
      propertyName: 'wo4w',
      template: 'custom/metrics-table-changes/wo4w',
      sortedBy: 'sortable_wo4w',
      title: 'Wo4W',
      disableFiltering: true,
      className: 'rootcause-metric__table__column'
    }
  ],

  /**
   * Data for metrics table
   * @type Object[] - array of objects, each corresponding to a row in the table
   */
  metricsTableData: computed(
    'urns',
    'selectedUrns',
    'entities',
    'changesOffset',
    function() {
      const { urns, entities, changesOffset, selectedUrns } =
        this.getProperties('urns', 'entities', 'changesOffset', 'selectedUrns');

      return urns.map(urn => {
        return {
          urn,
          isSelected: selectedUrns.has(urn),
          label: toMetricLabel(urn, entities),
          score: entities[urn].score.toFixed(2),
          wo1w: this._makeRecord('wo1w', urn),
          wo2w: this._makeRecord('wo2w', urn),
          wo3w: this._makeRecord('wo3w', urn),
          wo4w: this._makeRecord('wo4w', urn),
          sortable_wo1w: makeSortable(changesOffset['wo1w'][urn]),
          sortable_wo2w: makeSortable(changesOffset['wo2w'][urn]),
          sortable_wo3w: makeSortable(changesOffset['wo3w'][urn]),
          sortable_wo4w: makeSortable(changesOffset['wo4w'][urn])
        };
      });
    }
  ),

  _makeRecord(offset, urn) {
    const { entities, changesOffset, changesOffsetFormatted } =
      this.getProperties('entities', 'changesOffset', 'changesOffsetFormatted');
    return {
      value: changesOffsetFormatted[offset][urn],
      direction: toColorDirection(changesOffset[offset][urn], isInverse(urn, entities))
    };
  },

  /**
   * Keeps track of items that are selected in the table
   * @type {Array}
   */
  preselectedItems: computed(
    'metricsTableData',
    'selectedUrns',
    function () {
      const { metricsTableData, selectedUrns } = this.getProperties('metricsTableData', 'selectedUrns');
      const selectedEntities = [...selectedUrns].filter(urn => metricsTableData[urn]).map(urn => metricsTableData[urn]);
      return selectedEntities;
    }
  ),

  actions: {
    /**
     * Triggered on cell selection
     * Updates the currently selected urns based on user selection on the table
     * @param {Object} e
     */
    displayDataChanged (e) {
      const selectedItemsArr = [...e.selectedItems];
      const selectedItem = selectedItemsArr.length ? selectedItemsArr[0].urn : '';

      if (selectedItem) {
        this.get('toggleSelection')(selectedItem);
      }
    }
  }
});