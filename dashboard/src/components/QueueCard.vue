<template>
  <v-card>
    <Doughnut :data="chartData" :options="chartOptions"/>
  </v-card>
</template>

<script setup lang="ts">
  import { Doughnut } from 'vue-chartjs'
  import { Chart as ChartJS, ArcElement, Tooltip, Legend } from 'chart.js'

  ChartJS.register(ArcElement, Tooltip, Legend)

  const queue = ref({
    name: 'test',
    length: 0,
    deadletter: 0,
    checkout: 0
  });

  const chartData = {
    labels: ['Length', 'Deadletter', 'Checkout'],
    datasets: [
      {
        backgroundColor: ['#41B883', '#E46651', '#00D8FF'],
        data: [queue.length, queue.deadletter, queue.checkout]
      }
    ]
  };

  const chartOptions = {
    responsive: true,
    maintainAspectRatio: false
  }
</script>
