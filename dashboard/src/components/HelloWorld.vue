<template>
  <v-container class="fill-height">
      <!--<v-data-table :items="items" density="compact"
                    :headers="headers"
                    :items-per-page="0">
        <template #bottom></template>
      </v-data-table>-->
    <v-row>

      <v-col cols="12" md="4">
        <v-card>
          <Doughnut :data="chartData" :options="chartOptions"/>
        </v-card>
      </v-col>

      <v-col cols="12" md="4">
        <v-card>
          <Doughnut :data="chartData" :options="chartOptions"/>
        </v-card>
      </v-col>

      <v-col cols="12" md="4">
        <v-card>
          <Doughnut :data="chartData" :options="chartOptions"/>
        </v-card>
      </v-col>
    </v-row>



  </v-container>
</template>

<script setup lang="ts">
  import { Doughnut } from 'vue-chartjs'
  import { Chart as ChartJS, ArcElement, Tooltip, Legend } from 'chart.js'
  ChartJS.register(ArcElement,  Tooltip, Legend)

  const chartData = {
    labels: ['VueJs', 'EmberJs', 'ReactJs', 'AngularJs'],
    datasets: [
      {
        backgroundColor: ['#41B883', '#E46651', '#00D8FF', '#DD1B16'],
        data: [40, 20, 80, 10]
      }
    ]
  };

  const chartOptions = {
    responsive: true,
    maintainAspectRatio: false
  }

  const headers = [
    { title: 'Name', align: 'start', sortable: false, key: 'name' },
  ];

  const items = [
    {
      name: 'African Elephant',
      species: 'Loxodonta africana',
      diet: 'Herbivore',
      habitat: 'Savanna, Forests',
    },
  ];
</script>
